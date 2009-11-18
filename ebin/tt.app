{application, tt,
  [{description, "tiny table - a big table clone"},
   {vsn, "0.1.0"},
   {modules, [tt_server, tt_fsm, tt_pool, tt_cli]},
   {mod, {tt_app, []}},
   {env, [{local_ip, {127,0,0,1}}]},
   {registered, [tt_server, tt_pool]},
   {applications, [kernel, stdlib]}
  ]
}.
