-module(tt_db).

-behaviour(gen_server).

-compile(native).
-compile({hipe, [o3]}).

-compile(export_all).

-export([start/1]).
-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {servers = [], pport}).

start_link() -> start_link([]).

start_link(Config) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

start(Config) ->
  gen_server:start({local, ?MODULE}, ?MODULE, Config, []).

%%%----------------------------------------------------------------------
%%% Callback functions from gen_server
%%%----------------------------------------------------------------------

init() -> init([]).

init(Config) ->
  process_flag(trap_exit, true),
  {ok, PPort} = tt_cli:start_link(tt_db_client, {127,0,0,1}, 4400),
  {ok, #state{servers = Config, pport = PPort}}.

handle_call(ActionTuple, _From, #state{pport = PP} = State) ->
  Action = element(1, ActionTuple),
  Table  = element(2, ActionTuple),
  Key    = element(3, ActionTuple),
  SendKey = ["[", Table, "]", Key],  % we can send iolists.
  % 3 handles get
  % 4 handels set(key, val), fwmkeys(prefix, count).
  Result = case size(ActionTuple) of
             3 -> tt_cli:Action(PP, SendKey);
             4 -> tt_cli:Action(PP, SendKey, element(4, ActionTuple))
           end,
  {reply, Result, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(Info, State) ->
  io:format("Other info of: ~p~n", [Info]),
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%----------------------------------------------------------------------
%%% Usable Functions
%%%----------------------------------------------------------------------

set(Table, Key, Col, Val) when is_list(Table), is_atom(Col) ->
  set(Table, Key, atom_to_list(Col), Val);
set(Table, Key, Col, Val) when is_list(Table), is_list(Col) ->
  {A, B, C} = now(),
  Timestamp = <<(A * 1000000000000 + B * 1000000 + C):64>>,
  put(Table, [Key, " ", Col, " ", Timestamp], Val);
set(Table, Key, Col, Val) when is_atom(Table) ->
  set(atom_to_list(Table), Key, Col, Val).

put(Table, Key, Val) when is_list(Table) ->
  gen_server:call(?MODULE, {put, Table, Key, Val}).

get(Table, Key) when is_list(Table) ->
  gen_server:call(?MODULE, {get, Table, Key});
get(Table, Key) when is_atom(Table) ->
  get(atom_to_list(Table), Key).

fwmkeys(Table, Key, Count) when is_list(Table) ->
  gen_server:call(?MODULE, {fwmkeys, Table, Key, Count});
fwmkeys(Table, Key, Count) when is_atom(Table) ->
  fwmkeys(atom_to_list(Table), Key, Count).

fwmkeys_vals_dated(Table, Key, Count) when is_list(Table) ->
  Keys = fwmkeys(Table, Key, Count),
  Formatted = [{K, format_key(K)} || K <- Keys],
  [{OldNow, get(Table, K)} || {K, {_Readable, _TS, OldNow}} <- Formatted].

get_latest(Table, Key, Field) when is_list(Table), is_atom(Field) ->
  case fwmkeys(Table, [Key, " ", atom_to_list(Field)], 1) of
    [] -> notfound;
    [LatestKey] -> get(Table, LatestKey)
  end;
get_latest(Table, Key, Field) when is_atom(Table) ->
  get_latest(atom_to_list(Table), Key, Field).

get_latest_all(Table, Key, Field) when is_list(Table), is_atom(Field) ->
  case fwmkeys(Table, [Key, " ", atom_to_list(Field)], 1) of
    [] -> notfound;
    [LatestKey] -> {ReadableKey, TS, OldNow} = format_key(LatestKey),
                   {LatestKey, ReadableKey, TS, OldNow, get(Table, LatestKey)}
  end;
get_latest_all(Table, Key, Field) when is_atom(Table) ->
  get_latest_all(atom_to_list(Table), Key, Field).

format_key(Key) ->
  KeySize = size(Key) - 9,  % 9 = 8 byte TS + 1 space
  <<ReadableKey:KeySize/binary, _:8, TS:64>> = Key,
  OldNow = epoch_to_now(TS),
  {ReadableKey, TS, OldNow}.

readable_key(Key) ->
  KeySize = size(Key) - 9,  % 9 = 8 byte TS + 1 space
  <<ReadableKey:KeySize/binary, _:8, _:64>> = Key,
  ReadableKey.

readable_col(Username, Key) ->
  Sz = length(Username) + 1,
  KeySize = size(Key) - 9 - Sz,  % 9 = 8 byte TS + 1 space
  <<_:Sz/binary, ReadableKey:KeySize/binary, _:8, _:64>> = Key,
  ReadableKey.

epoch_to_now(Epoch) when Epoch < 1000000000000 ->
  OldNowA = trunc(Epoch / 1000000),
  NewTSA  = Epoch - OldNowA * 1000000,
  OldNowB = trunc(NewTSA),
  {OldNowA, OldNowB, 500000};  % on average it's half through the usec!
epoch_to_now(Epoch) when Epoch > 1000000000000 ->
  OldNowA = trunc(Epoch / 1000000000000),
  NewTSA  = Epoch - OldNowA * 1000000000000,
  OldNowB = trunc(NewTSA / 1000000),
  NewTSB  = NewTSA - OldNowB * 1000000,
  OldNowC = trunc(NewTSB),
  {OldNowA, OldNowB, OldNowC}.
