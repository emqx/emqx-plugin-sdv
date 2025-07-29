-module(emqx_sdv_fanout_inflight_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    [
        F
     || {F, _} <- ?MODULE:module_info(exports),
        is_test_function(F)
    ].

is_test_function(F) ->
    case atom_to_list(F) of
        "t_" ++ _ -> true;
        _ -> false
    end.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

t_no_leak(_Config) ->
    {ok, _} = emqx_sdv_fanout_inflight:start_link(),
    {Pid, MRef} = spawn_monitor(fun() ->
        link(whereis(emqx_sdv_fanout_inflight)),
        emqx_sdv_fanout_inflight:insert(self(), 1, self(), make_ref()),
        ?assert(emqx_sdv_fanout_inflight:exists(self())),
        exit(normal)
    end),
    receive
        {'DOWN', MRef, process, Pid, Reason} ->
            ?assertEqual(normal, Reason)
    end,
    %% Make a random synced call to ensure the EXIT message is processed
    _ = gen_server:call(emqx_sdv_fanout_inflight, foobar, infinity),
    ?assertNot(emqx_sdv_fanout_inflight:exists(Pid)),
    ok = emqx_sdv_fanout_inflight:stop(),
    ok.
