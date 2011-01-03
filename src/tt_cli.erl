%% This is Ulf's Tokyo Tyrant client
%% I cleaned up the syntax to make it more tolerable.
%% Not sure why it's not at https://github.com/uwiger

-module(tt_cli).

-behaviour(gen_server).

-export([start_link/3, start_link/6]).
-export([put/3, get/2, mget/2, fwmkeys/3]).

%% internal exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
         terminate/2, code_change/3]).

-compile(export_all).

%% By default we don't encode or decode anything. Just pass everything through.
-record(state, {socket = [],
                encode = fun(E) -> E end,
                decode = fun(E) -> E end,
                encode_prefix = fun(E) -> E end}).

start_link(Host, Port) when is_tuple(Host), is_integer(Port) ->
  gen_server:start_link(?MODULE, {Host, Port}, []).

start_link(Name, Host, Port) when is_tuple(Host), is_integer(Port) ->
  gen_server:start_link({local, Name}, ?MODULE, {Host, Port}, []).

start_link(Name, Host, Port, Encode, EncodePrefix, Decode) when 
    is_tuple(Host), is_integer(Port),
    is_function(Encode), is_function(EncodePrefix), is_function(Decode) ->
  gen_server:start_link({local, Name}, ?MODULE, 
                        {Host, Port, Encode, EncodePrefix, Decode}, []).

init({Host, Port}) ->
  case gen_tcp:connect(Host, Port, [binary, {active,false}, {nodelay,true}]) of
    {ok, Socket} -> {ok, #state{socket = Socket}};
    Error -> Error
  end;
init({Host, Port, Encode, EncodePrefix, Decode}) ->
  case gen_tcp:connect(Host, Port, [binary, {active,false}, {nodelay,true}]) of
    {ok, Socket} -> {ok, #state{socket = Socket,
                                encode = Encode,
                                encode_prefix = EncodePrefix,
                                decode = Decode}};
    Error -> Error
  end.

handle_call({encode, Term}, _From, #state{encode = Encode} = S) ->
  {reply, Encode(Term), S};
handle_call({decode, Term}, _From, #state{decode = Decode} = S) ->
  {reply, Decode(Term), S};
handle_call({encode_prefix, Term}, _From, #state{encode_prefix = Prefix} = S) ->
  {reply, Prefix(Term), S};
handle_call({cmd, Req}, _From, #state{socket = Sock} = S) ->
  Msg = mk_req(Req),
  gen_tcp:send(Sock, Msg),
  Reply = cmd_reply(Sock),
  {reply, Reply, S};
handle_call({ask, Req}, _From, #state{socket = Sock} = S) ->
  Msg = mk_req(Req),
  gen_tcp:send(Sock, Msg),
  Reply = ask_reply(Req, Sock),
  {reply, Reply, S}.

handle_info(Msg, S) ->
  io:format("handle_info(~p, ~p)~n", [Msg, S]),
  {noreply, S}.

handle_cast(_, S) ->
  {stop, unknown_cast, S}.

terminate(Reason, S) ->
  io:format("terminate(~p, ~p)~n", [Reason, S]).

code_change(_FromVsn, S, _Extra) ->
  {ok, S}.


put(TT, Key, Value) ->
  cmd(TT, {put, encode(TT, Key), encode(TT, Value)}).

get(TT, Key) ->
  case ask(TT, {get, encode(TT, Key)}) of
    {ok, Vb} -> decode(TT, Vb);
    Err -> throw({?MODULE, get, Err})
  end.

mget(TT, Keys) when is_list(Keys) ->
  Enc = [encode(TT, K) || K <- Keys],
  case ask(TT, {mget, Enc}) of
    {ok, KVs} -> [{decode(TT, K), decode(TT, V)} || {K,V} <- KVs];
    Err -> throw({?MODULE, mget, Err})
  end.

fwmkeys(TT, Prefix, Limit) ->
  case ask(TT, {fwmkeys, encode_prefix(TT, Prefix), Limit}) of
    {ok, Keys} -> [decode(TT, K) || K <- Keys];
    Err -> throw({?MODULE, fwmkeys, Err})
  end.


cmd(TT, Req) ->
  gen_server:call(TT, {cmd, Req}).

ask(TT, Req) ->
  gen_server:call(TT, {ask, Req}).

encode(TT, Term) ->
  gen_server:call(TT, {encode, Term}).
%  sext:encode(Term).

decode(TT, Bin) ->
  gen_server:call(TT, {decode, Bin}).
%  sext:decode(Bin).

encode_prefix(TT, Term) ->
  gen_server:call(TT, {encode_prefix, Term}).
%  sext:prefix(Term).


mk_req({put, K, V}) ->
  KSz = iolist_size(K),
  VSz = iolist_size(V),
  [<<16#c8, 16#10, KSz:32, VSz:32>>, K, V];
mk_req({get, K}) ->
  KSz = iolist_size(K),
  [<<16#c8, 16#30, KSz:32>>, K];
mk_req({mget, Ks}) ->
  N = length(Ks),
  Packed = pack_values(Ks),
  [<<16#c8, 16#31, N:32>>, Packed];
mk_req({fwmkeys, Prefix, Limit}) ->
  PSz = iolist_size(Prefix),
  [<<16#c8, 16#58, PSz:32, Limit:32>>, Prefix].

pack_values(Values) ->
  pack_values(Values, <<>>).

pack_values([H|T], Acc) ->
  Sz = iolist_size(H),
  Bin = <<Sz:32, H/binary>>,
  pack_values(T, <<Acc/binary, Bin/binary>>);
pack_values([], Acc) ->
  Acc.


cmd_reply(Sock) ->
  case gen_tcp:recv(Sock, 1) of
    {ok, <<0>>} -> ok;
    {ok, <<E>>} -> {error, E};
    {error,_} = Err -> Err
  end.

ask_reply(Req, Sock) ->
  Method = element(1, Req),
  case gen_tcp:recv(Sock, 0) of
    {ok, <<0, Rest/binary>>} -> 
      try 
        get_reply(Method, Rest, Sock)
      catch
        throw:{error,Reason} -> {error, Reason}
      end;
    {ok, <<E>>} -> {error, E};
    {error,_} = Err -> Err
  end.

get_reply(get, Data, Sock) ->
  {Val, _} = get_value(Data, Sock),
  {ok, Val};
get_reply(mget, Data, Sock) ->
  {N, D1} = get_word(Data, Sock),
  Result = get_N(N, D1, fun get_k_v/2, Sock),
  {ok, Result};
get_reply(fwmkeys, Data, Sock) ->
  {N, D1} = get_word(Data, Sock),
  Result = get_N(N, D1, fun get_value/2, Sock),
  {ok, Result}.

get_word(<<W:32, Rest/binary>>, _Sock) ->
  {W, Rest};
get_word(Sofar, Sock) ->
  Bin = get_data(Sock),
  get_word(<<Sofar/binary, Bin/binary>>, Sock).

get_value(<<Sz:32, V:Sz/binary, Rest/binary>>, _Sock) ->
  {V, Rest};
get_value(Sofar, Sock) ->
  Bin = get_data(Sock),
  get_value(<<Sofar/binary, Bin/binary>>, Sock).

get_k_v(<<KSz:32, VSz:32, K:KSz/binary, V:VSz/binary, Rest/binary>>, _Sock) ->
  {{K,V}, Rest};
get_k_v(Sofar, Sock) ->
  Bin = get_data(Sock),
  get_k_v(<<Sofar/binary, Bin/binary>>, Sock).

get_N(0, _, _, _) ->
  [];
get_N(N, Data, F, Sock) when N > 0 ->
  {Item, Rest} = F(Data, Sock),
  [Item | get_N(N-1, Rest, F, Sock)].

get_data(Sock) ->
  case gen_tcp:recv(Sock, 0) of
    {ok, Bin} -> Bin;
    {error,_} = Err -> throw(Err)
  end.
    
