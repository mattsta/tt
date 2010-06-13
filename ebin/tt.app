{application,tt,
             [{description,"tiny table - a big table clone"},
              {vsn,"0.0.1"},
              {modules,[tt_cli,tt_fsm,tt_pool,tt_server]},
              {mod,{tt_app,[]}},
              {env,[{local_ip,{127,0,0,1}}]},
              {registered,[tt_server,tt_pool]},
              {applications,[kernel,stdlib]}]}.
