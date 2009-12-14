-module(tt_fsm).
-behaviour(gen_fsm).

-export([start_link/1]).
-export([init/1]).

-compile(export_all).

-export([handle_info/3, terminate/3, handle_event/3, handle_sync_event/4,
         code_change/4]).

-export([ready/2, ready_for_data/2, fetching_data/2]).

-record(state, {sock, ip, port, packet, data = <<>>, table,
                table_socket_map = dict:new()}).


start_link(ClientSocket) -> gen_fsm:start_link(?MODULE, ClientSocket, []).

init(ClientSocket) ->
  process_flag(trap_exit, true),
  {ok, RE} = re:compile("^.*?]"),
  put(key_regex, RE),
  {ok, ready, #state{sock=ClientSocket}, 3000}.

ready(socket_ready, #state{sock=Socket} = State) ->
  inet:setopts(Socket, [{active, once}, {packet, raw}, binary]),
  {ok, {IP, Port}} = inet:peername(Socket),
  % the client can idle, so don't time them out here.
  {next_state, ready_for_data, State#state{ip = IP, port = Port}, infinity};
ready(timeout, State) ->
  io:format("Timeout in ready: ~p~n", [State]),
  {stop, normal, State}.

ready_for_data(data_in_process, #state{data=Data} = State) ->
  case find_table(Data) of
    {error, _, _} -> {next_state, data_in_process, State};
    {Table, NewPacket,R} -> NewState = 
                            State#state{table=Table, 
                                        packet=NewPacket,
                                        data = R},
                            ready_for_data(finished_parsing_packet, NewState);
    unsupported_protocol -> ready_for_data(unsupported_protocol, State);
    unsupported_command  -> ready_for_data(unsupported_command, State)
  end;
ready_for_data(finished_parsing_packet, 
               #state{table=Table, packet=Data, data=NextData,
               table_socket_map = Map} = State) ->
  {Socket, NewMap} = 
  case dict:find(Table, Map) of
    error -> NewSocket = tt_pool:get_socket(self(), Table),
             case NewSocket of
               timeout -> exit(no_socket);
                     _ -> {NewSocket, dict:store(Table, NewSocket, Map)}
             end;
    {ok, FoundSocket} -> {FoundSocket, Map}
  end,
  ok = gen_tcp:send(Socket, Data),
%  io:format("current data: ~p; next data: ~p~n", [Data, NextData]),
  {next_state, fetching_data, 
   State#state{packet = <<>>, data = NextData, table_socket_map = NewMap}, 
   infinity};  % let them idle here too, damnit
ready_for_data(timeout, State) ->
  error_logger:error_msg("Timeout received from frontend: ~p.~n", [State]),
  {stop, normal, State};
ready_for_data(unsupported_protocol, State) ->
  error_logger:error_msg("Unsupported protocol from frontend: ~p.~n", [State]),
  {stop, normal, State};
ready_for_data(unsupported_command, State) ->
  error_logger:error_msg("Unsupported command from frontend: ~p.~n", [State]),
  {stop, normal, State}.

fetching_data({fetched, Data, _BackendSocket}, #state{sock=Socket} = State) ->
  %% Need to force order to be preserved.
  %% Example: Request in -> [foo]abc; [jack]def
  %%          Request back -> from jack, then from foo.  client expected orignal
  %%          ordering.  Store ordered list of outstanding sockets waiting for?
  gen_tcp:send(Socket, Data),
  {next_state, fetching_data, State, infinity};  % MORE IDLING
fetching_data(data_in_process, State) ->
  ready_for_data(data_in_process, State);
fetching_data(timeout, State) ->
  error_logger:error_msg("Timeout receiving from backend: ~p.~n", [State]),
  {stop, normal, State};
fetching_data(finished, State) ->
  {stop, normal, State}.

handle_info({tcp, Socket, Bin}, StateName, #state{sock=Socket, data=Data}=S) ->
  %% Receive data from the front end
  inet:setopts(Socket, [{active, once}]),
  NewData = <<Data/binary, Bin/binary>>,
  ?MODULE:StateName(data_in_process, S#state{data=NewData});
handle_info({tcp, BackendSocket, Data}, StateName, State) ->
  %% Receive data from a non-front-end (i.e. backend) socket.
  ?MODULE:StateName({fetched, Data, BackendSocket}, State);
handle_info({tcp_closed, Socket}, _StateName, #state{sock=Socket}=S) ->
  {stop, normal, S};
handle_info(Msg, _StateName, State) ->
  io:format("A Message: ~p~n", [Msg]),
  {stop, normal, State}.

find_table(<<16#C8:8, Rest/binary>>) -> find_tc_table(Rest);
find_table(_) -> unsupported_protocol.
  
find_tc_table(<<Cmd:8, _/binary>> = Command) ->
  try extract_tc_table(Command) of
    {Table, NewPacket, R} -> {Table, <<16#C8, Cmd, NewPacket/binary>>, R};
              unsupported -> unsupported_command
  catch
    %% If the packet isn't complete yet or if it's unknown then do nothing.
    error:E -> {error, E, erlang:get_stacktrace()}
  end.

extract_tc_table(<<16#58:8, Rest/binary>>) -> pattern_c(Rest);  % fwmkeys
extract_tc_table(<<16#18:8, Rest/binary>>) -> pattern_a(Rest);  % putnr
extract_tc_table(<<16#30:8, Rest/binary>>) -> pattern_b(Rest);  % get
extract_tc_table(<<16#10:8, Rest/binary>>) -> pattern_a(Rest);  % put
extract_tc_table(<<16#11:8, Rest/binary>>) -> pattern_a(Rest);  % putkeep
extract_tc_table(<<16#12:8, Rest/binary>>) -> pattern_a(Rest);  % putcat
extract_tc_table(<<16#20:8, Rest/binary>>) -> pattern_b(Rest);  % out
extract_tc_table(<<16#60:8, Rest/binary>>) -> pattern_c(Rest);  % addint
extract_tc_table(<<16#68:8, Rest/binary>>) -> pattern_d(Rest);  % ext
extract_tc_table(<<16#73:8, Rest/binary>>) -> pattern_b(Rest);  % copy
extract_tc_table(<<_:8, Rest/binary>>) -> {[], Rest, <<>>};
extract_tc_table(_) -> unsupported.  % asking for something we won't do


pattern_a(<<KeySz:32/integer, ValSz:32/integer, 
            Key:KeySz/binary, Val:ValSz/binary, Rest/binary>>) ->
  {NewKeySz, TableName, NewKey} = process_key(Key, KeySz),
  {TableName, <<NewKeySz:32/integer, ValSz:32, 
                NewKey:NewKeySz/binary, Val:ValSz/binary>>, Rest}.

pattern_b(<<KeySz:32/integer, Key:KeySz/binary, Rest/binary>>) ->
  {NewKeySz, TableName, NewKey} = process_key(Key, KeySz),
  {TableName, <<NewKeySz:32/integer, NewKey:NewKeySz/binary>>, Rest}.

pattern_c(<<KeySz:32/integer, Count:32/integer, Key:KeySz/binary, 
            Rest/binary>>) ->
  {NewKeySz, TableName, NewKey} = process_key(Key, KeySz),
  {TableName, <<NewKeySz:32/integer, Count:32/integer, 
                NewKey:NewKeySz/binary>>, Rest}.

pattern_d(<<FnSz:32/integer, Opts:32/integer, KeySz:32/integer, 
            ValSz:32/integer, Fn:FnSz/binary, Key:KeySz/binary, 
            Val:ValSz/binary, Rest/binary>>) ->
  {NewKeySz, TableName, NewKey} = process_key(Key, KeySz),
  {TableName, <<FnSz:32/integer, Opts:32/integer, NewKeySz:32/integer, 
                ValSz:32/integer, Fn:FnSz/binary, NewKey:NewKeySz/binary,
                Val:ValSz/binary>>, Rest}.

extract_table_name(CombinedKey) ->
  case lists:foldl(fun extract_table_name/2, [], CombinedKey) of
    {Table, NewKey} -> {lists:reverse(Table), lists:reverse(NewKey)};
                  _ -> {[], CombinedKey}
  end.

extract_table_name($[, Table)        -> Table;
extract_table_name($], Table)        -> {Table, []};
extract_table_name(E,  {Table, Key}) -> {Table, [E | Key]};
extract_table_name(E,  Table)        -> [E | Table].

extract_table_name_re(Key) ->
  case re:run(Key, get(key_regex)) of
    {match, [{0, Length}]} -> {string:substr(Key, 2, Length - 2),
                               string:substr(Key, Length + 1)};
    nomatch -> {[], Key}
  end.

extract_table_name_str(Key) ->
  case string:str(Key, "]") of
    0 -> {[], Key};
    N -> {string:substr(Key, 2, N - 2), string:substr(Key, N + 1)}
  end.

process_key(Key, KeySz) ->
  KeyList = binary_to_list(Key),
  {TableNameSz, TableName, NewKey} = 
  case hd(KeyList) of
    $[ -> case extract_table_name_str(KeyList) of
               {[], NKey} -> {0, [], NKey};
            {TName, NKey} -> {length(TName) + 2, TName, NKey}
          end;
     _ -> {0, [], KeyList}
    end,
  NewKeySz = KeySz - TableNameSz,
  {NewKeySz, TableName, list_to_binary(NewKey)}.


code_change(_, _, _, _) -> ok.
handle_event(_, _, _) -> ok.
handle_sync_event(_, _, _, _) -> ok.

terminate(_Reason, _StateName, #state{sock=Socket, table_socket_map=Map}) ->
  dict:map(fun(_TableName, TableSocket) when is_port(TableSocket) ->
             %% Give the socket back to tt_pool.
             %% When the socket closes, tt_pool will make a new socket for the 
             %% table and perform all proper accounting.
             gen_tcp:controlling_process(TableSocket, whereis(tt_pool)),
             gen_tcp:close(TableSocket);
           (_, _) -> ok
           end, Map),
  case Socket of
    undefined -> ok;
    Socket when is_port(Socket) -> gen_tcp:close(Socket)
  end,
  ok.
