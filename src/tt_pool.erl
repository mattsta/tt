-module(tt_pool).

-compile(export_all).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export([terminate/2, code_change/3]).


start_link(Tables) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Tables, []).

% accept list of [{table_name, ip, port}]
-type table_name() :: string().
-type ip() :: string().
-type tcp_port() :: 1024..65535.
-type table_spec() :: {table_name(), ip(), tcp_port()}.
-spec init([table_spec(),...]) -> {ok, []}.

init(Tables) ->
  process_flag(trap_exit, true),
  crypto:start(),
  % Stores {table_name, ip, port, queue} associations
  ets:new(tt_pool_table, [named_table, public]),
  ets:new(tt_pool_queue, [named_table, public]),
  ets:new(tt_pool_socket, [named_table, public]),
  ets:new(tt_pool_table_failed, [named_table, public]),
  [new_table(Table) || Table <- Tables],
  {ok, []}.

new_table([{TableName, IP, Port}]) ->
  new_table(TableName, IP, Port);
new_table({TableName, IP, Port}) ->
  new_table(TableName, IP, Port).

new_table(TableName, IP, Port) ->
  TableInfo = {TableName, IP, Port},
  true = ets:insert(tt_pool_table, TableInfo),
  GatherResult = 
  case insert_gather_queue(TableName) of
    server_not_listening -> true = ets:insert(tt_pool_table_failed, TableInfo),
                            timer:apply_after(3000, ?MODULE, new_table, 
                                               [TableName, IP, Port]),
                            table_failed;
            % if previously failed, delete it.  If not, no harm done.
    true -> true = ets:delete(tt_pool_table_failed, TableName),
            ok
  end,
  io:format("Gather result: (~p, ~p)~n", [TableInfo, GatherResult]),
  GatherResult.

insert_gather_queue(TableName) ->
  {TableName, Q} = gather_queue(TableName),
  case queue:is_empty(Q) of
    true -> server_not_listening;
    false -> true = ets:insert(tt_pool_queue, {TableName, Q})
  end.

gather_queue(TableName) ->
  PreSocks = [socket_for_table(TableName) || _ <- lists:seq(1,8)],

  % emfile means we've gone past our ulimit.  bad.
  OkSocks = [S || S <- PreSocks, S =/= {error, econnrefused},
                                 S =/= {error, emfile}],

  Q = lists:foldl(fun({ok, Sock}, Q) -> 
                    true = ets:insert(tt_pool_socket, {Sock, TableName}),
                    queue:in(Sock, Q)
                  end, queue:new(), OkSocks),
  {TableName, Q}.

handle_call({get_socket, FromPid, TableName}, _From, State) ->
  [{Table, Q}] = 
  case ets:lookup(tt_pool_queue, TableName) of
    %% If the talbe isn't found, let's just use the first one.  Good for
    %% testing by using an unmodified tcrmgr
    [] -> ets:lookup(tt_pool_queue, ets:first(tt_pool_queue));
     F -> F
  end,
  case queue:out(Q) of
    {{value, Sock}, NQ} -> true = ets:insert(tt_pool_queue, {Table, NQ}), 
                           case gen_tcp:controlling_process(Sock, FromPid) of
                                     ok -> {reply, Sock, State};
                             {error, _} -> remove_socket(Sock, out),
                                      % re-try to deliver a valid socket now
                                      tt_pool:handle_call({get_socket, FromPid, 
                                                    TableName}, _From, State)
                           end;
    {empty, Q} -> {reply, [], State}
  end;

%% Add a new socket back when one is closed.
%% Usually called by trapping exit of the port forced closed by the front end
handle_call({add_socket, Table}, _From, State) ->
  [{Table, Q}] = ets:lookup(tt_pool_queue, Table),
  SocketResult = 
  case socket_for_table(Table) of
    {ok, Sock} -> NQ = queue:in(Sock, Q),
                  true = ets:insert(tt_pool_socket, {Sock, Table}),
                  true = ets:insert(tt_pool_queue, {Table, NQ});
    {error, closed} -> drop_socket;
    {error, econnrefused} -> drop_socket
  end,
  % If the socket failed to connect, let's retry after 3ish seconds.
  ResultState = 
  case SocketResult of
    drop_socket -> RandomDelay = crypto:rand_uniform(100, 1000) + 3000,
                   timer:apply_after(RandomDelay, ?MODULE, add_socket, [Table]);
           true -> ok
  end,
  {reply, ResultState, State};

handle_call(Call, _From, State) ->
  io:format("Suprrious call: ~p~n", [Call]),
  {reply, Call, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info({'EXIT', Socket, normal}, State) when is_port(Socket) ->
  % lookup which table socket belonged to
  % assign new socket for table
  remove_socket(Socket, exit),
  {noreply, State};
handle_info({'EXIT', _, _}, State) ->
  % something else exited.  Ignore it.  (from a linked process)
  {noreply, State};
handle_info({tcp_closed, Socket}, State) when is_port(Socket) ->
%  io:format("Closed for: ~p~n", [Socket]),
  % Get table the socket belonged to.  If no sockets left, re-schedule table
  % for initialization.
  remove_socket(Socket, closed),
  {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVersion, State, _Extra) -> {ok, State}.

socket_for_table(TableName) -> 
  [{TableName, IP, Port}] = ets:lookup(tt_pool_table, TableName),
  gen_tcp:connect(IP, Port, [binary, {active, true}]).

% Whence is out || exit || closed.  exit is the case we care about.
remove_socket(Socket, out) ->
  % this is a lazy way of hiding a bug I haven't bothered to track down yet.
  % somehow, we are giving remove_socket a socket that isn't in the
  % tt_pool_socket table.
  % reproduce: start and connect, stop tyrant, re-start tyrant and have
  % tt-pool reconnect everything.  now, some Socket doesn't exist in 
  % tt_pool_socket 

  % the 'out' is called because a socket doesn't have an owning PID.
  % see: handle_call for get_socket under the controlling_pid check.
  (catch remove_socket(Socket, out_second));
remove_socket(Socket, Whence) -> 
  FoundSocket = ets:lookup(tt_pool_socket, Socket),
%  io:format("Removing found socket of: ~p~n", [FoundSocket]),
  [{Socket, TableName}] = FoundSocket,
  ets:delete(tt_pool_socket, Socket),
  case ets:lookup(tt_pool_queue, TableName) of
    {TableName, Q} -> case queue:is_empty(Q) of
                        true -> TabInfo = ets:lookup(tt_pool_table, TableName),
                                new_table(TabInfo);
                        false ->  ok
                      end,
                      case Whence of
                        exit -> RemSocketFun = fun(S) when S == Socket -> false;
                                                  (_) -> true
                                               end,
                                RemSockQueue = queue:filter(RemSocketFun, Q),
                                ets:insert(tt_pool_socket, 
                                  {TableName, RemSockQueue});
                           _ -> ok
                      end;
                 _ -> odd_failure
  end,
  timer:apply_after(300, ?MODULE, add_socket, [TableName]).

% Module usages

remove_table(TableName) ->
  %% This should be in a gen_server call so it's locked against current reads
  %% Also only remove when current reads are done?
  [{TableName, Q}] = ets:lookup(tt_pool_queue, TableName),
  SocketsForTable = queue:to_list(Q),
  [gen_tcp:close(S) || S <- SocketsForTable],
  [ets:delete(tt_pool_queue, S) || S <- SocketsForTable],
  ok.

get_socket(FromPid, Table) ->
  get_socket(FromPid, Table, 0).

get_socket(_FromPid, _Table, Count) when Count > 10 ->
  timeout;
get_socket(FromPid, Table, Count) when Count =< 10 ->
  case gen_server:call(?MODULE, {get_socket, FromPid, Table}) of
    [] -> timer:sleep(200),
          get_socket(FromPid, Table, Count + 1);
     S -> S
  end.

add_socket(TableName) ->
  gen_server:call(?MODULE, {add_socket, TableName}).

