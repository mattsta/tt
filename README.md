tt: tiny table
==============

What is tt?
-----------
tiny table is a big table clone based around being a proxy for tokyo tyrant.

tiny table allows multiple tyrant servers to be
accessed from a single address.  The tiny table server acts as a tyrant server with only
a subset of tyrant commands.  The table you access is prepended to each
key you read/write as: [tablename]key.  So [users]bob accesses key bob on
the users proxy and [accounts]6612 would access key 6612 on tyrant server
associated with accounts.

Status
------
I made tt in October 2009 to test the practicality of muxing tokyo tyrant servers
in a big-table-esque data model.
It worked well enough, but other faster, better documented, and better maintained
backends have been created since then.

This code is unsupported and there are no plans to improve it in the future.

So what good is it?
-------------------
Public code is better than code rotting in binary darkness.  I present tiny table for your entertainment.

`tt_fsm.erl` is a gen_fsm for passing through a subset of tokyo tyrant commands
to a backend server.  See `extract_tc_table/2` for the supported
commands.  The FSM is also a good example of parsing binary protocols with
erlang (see pattern_{a,b,c,d} functions).

`tt_pool.erl` is a gen_server to pool TCP connections to tyrant backends.

`tt_server.erl` is a gen_server to listen for clients to access backend
tyrant servers.  `tt_server` is is built around the "Building a non-blocking TCP server"
example floating around trapexit.  The `tt_server` has no coupling to tyrant,
ior the data model. All it does is start a new Module (in this case `tt_fsm`)
for each connection (passing in the established ClientSocket).

`tt_cli.erl` is Ulf Wiger's tokyo tyrant client.  I've modified it to fit my syntax
sensibilities better.

`tt_db.erl` is simple and complicated with a purpose.
It is where the "big table clone" data model happens.
Big table summary: keys are tri-valued.  A key is (Key, Column, 64-bit Timestamp).
`tt_db` does the [bracket] table processing for you and it auto-appends a (binary!)
64-bit timestamp to every key you store.  Each time you store something, your 
new data is *appended* to
the previous values so every prior value is available as well (i.e. it gives you the 3D
time aspect of tables).  Because `tt_db` stores *everything* what you want most
of the time is tt_db:get_latest (which returns the most-recent value stored to 
the Key, Column you are requesting).
You can see how tt_db, tyrant, the proxy, and big table fit together below:
    % set_user/3 and get_user/2 are wrapper functions so we are not including
    % the table name "user" in every call to get/set.
    set_user(Username, Field, Value) ->
      tt_db:set("user", Username, Field, Value).  % timestamp auto-appended on set

    get_user(Username, Field) ->
      tt_db:get_latest("user", Username, Field).  % value returned from most recent set

    set_name(Username, Name) -> set_user(Username, name, Name).
    set_age(Username, Age) -> set_user(Username, age, Age).
    set_gender(Username, Gender) -> set_user(Username, gender, Gender).

    get_name(Username) -> get_user(Username, name).
    get_age(Username) -> get_user(Username, age).
    get_gender(Username) -> get_user(Username, gender).

    set_status(Username, Status) -> setu(Username, status, Status).
    % fwmkeys_vals_dated returns the last Count stored items *with* timestamps
    get_status(Username, Count) ->
      Keys = tt_db:fwmkeys_vals_dated("user", Username ++ " status", Count),
      [{OldNow, binary_to_list(Val)} || {OldNow, Val} <- Keys].

    % fwmkeys returns the last Count (in this case 256) items *without* timestamps
    get_simple_quests() ->
      Simple = tt_db:fwmkeys(quests, "simple", 256),
      [tt_db:get(quests, X) || X <- Simple].


Usage
-----
First, build with rebar:
    rebar compile

Start the proxy server and give it the name of a module to pass client sockets to:
    tt_server:start_link(4400, tt_fsm).

Start the backend connections to tokyo tyrant:
    tt_pool:start_link([{"foo", "127.0.0.1", 2020}]).

Port 4400 is where the proxy listens for connections.
127.0.0.1:2020 is a tokyo tyrant database "foo"

You can test the proxy directly with the tokyo tyrant benchmark commands. If
a key is not prefixed with a table name (such as when using non-tt aware
benchmarking tools), the client is sent to the
first tokyo tyrant backend listed in your tt_pool:start_link/1 list.

The list passed to tt_pool:start_link/1 is all the tables
the proxy will activate on startup.  [TableName, IP, TyrantPort].
e.g.:
      [{"foo", "127.0.0.1", 2020},
       {"bar", "127.0.0.1", 2021},
       {"baz", "127.0.0.1", 2022}]

Use the table name in brackets before your key so
the proxy knows where to route your requests: [bar]my_key.  The table bracketing
is taken care of automatically if you use `tt_db`.


Contributions
-------------
I consider this project dead, but feel free to submit patches if you want to make the world a better place.
