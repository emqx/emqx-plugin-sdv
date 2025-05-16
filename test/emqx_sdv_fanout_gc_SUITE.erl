-module(emqx_sdv_fanout_gc_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(RETENTION, 1000).
-define(SCAN_LIMIT, 9).
-define(SCAN_DELAY, 1).

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
    %% speed up the test by setting a small scan limit and delay
    persistent_term:put(emqx_sdv_fanout_gc_scan_limit, ?SCAN_LIMIT),
    persistent_term:put(emqx_sdv_fanout_gc_scan_delay, ?SCAN_DELAY),
    meck:new(emqx_sdv_config, [passthrough]),
    meck:expect(emqx_sdv_config, get_data_retention, fun() -> ?RETENTION end),
    meck:expect(emqx_sdv_config, get_gc_interval, fun() -> 1000 end),
    Config.

end_per_testcase(_Case, _Config) ->
    meck:unload(emqx_sdv_config),
    ok.

t_continue_gc_ids_table(_Config) ->
    Tester = self(),
    meck:new(emqx_sdv_fanout_ids, [passthrough]),
    %% always return continue
    meck:expect(emqx_sdv_fanout_ids, gc, infinite_gc_loop_fn(Tester, ids)),
    try
        emqx_sdv_fanout_gc:start_link(),
        %% trigger gc immediately
        emqx_sdv_fanout_gc:run(),
        %% expect gc_begin, then endless 'continue' messages,
        %% we only check 10
        ExpectedMsgs = [gc_begin | lists:seq(1, 10)],
        lists:foreach(
            fun(Msg) ->
                receive
                    {next, ids, Msg} ->
                        ok;
                    Other ->
                        error({unexpected_message, Other})
                end
            end,
            ExpectedMsgs
        )
    after
        ok = gen_server:stop(emqx_sdv_fanout_gc),
        meck:unload(emqx_sdv_fanout_ids)
    end.

t_continue_gc_data_table(_Config) ->
    Tester = self(),
    meck:new(emqx_sdv_fanout_ids, [passthrough]),
    %% mock empty ids table
    meck:expect(emqx_sdv_fanout_ids, gc, fun(_Next, _Limit, _ExpireAt) -> complete end),
    meck:new(emqx_sdv_fanout_data, [passthrough]),
    %% mock large amount of data
    meck:expect(emqx_sdv_fanout_data, gc, infinite_gc_loop_fn(Tester, data)),
    try
        emqx_sdv_fanout_gc:start_link(),
        %% trigger gc immediately
        emqx_sdv_fanout_gc:run(),
        %% expect gc_begin, then endless 'continue' messages,
        %% we only check 10
        ExpectedMsgs = [gc_begin | lists:seq(1, 10)],
        lists:foreach(
            fun(Msg) ->
                receive
                    {next, data,Msg} ->
                        ok;
                    Other ->
                        error({unexpected_message, Other})
                end
            end,
            ExpectedMsgs
        )
    after
        ok = gen_server:stop(emqx_sdv_fanout_gc),
        meck:unload(emqx_sdv_fanout_ids),
        meck:unload(emqx_sdv_fanout_data)
    end.

t_gc_ids_then_data(_Config) ->
    Tester = self(),
    meck:new(emqx_sdv_fanout_ids, [passthrough]),
    %% mock 10 iterations for ids table
    meck:expect(emqx_sdv_fanout_ids, gc, gc_loop_fn(Tester, ids,10)),
    meck:new(emqx_sdv_fanout_data, [passthrough]),
    %% mock 10 iterations for data
    meck:expect(emqx_sdv_fanout_data, gc, gc_loop_fn(Tester, data, 10)),
    try
        emqx_sdv_fanout_gc:start_link(),
        %% trigger gc immediately
        emqx_sdv_fanout_gc:run(),
        %% expect gc_begin, then endless 'continue' messages
        ExpectedMsgs1 = [{ids, Next1} || Next1 <- [gc_begin | lists:seq(1, 10)]],
        ExpectedMsgs2 = [{data, Next2} || Next2 <- [gc_begin | lists:seq(1, 10)]],
        lists:foreach(
            fun({Tab, Msg}) ->
                receive
                    {next, Tab, Msg} ->
                        ok;
                    Other ->
                        error({unexpected_message, Other})
                after
                    3000 ->
                        error(timeout)
                end
            end,
            ExpectedMsgs1 ++ ExpectedMsgs2
        )
    after
        ok = gen_server:stop(emqx_sdv_fanout_gc),
        meck:unload(emqx_sdv_fanout_ids),
        meck:unload(emqx_sdv_fanout_data)
    end.

infinite_gc_loop_fn(Tester, Tab) ->
    gc_loop_fn(Tester, Tab, 100000000).

gc_loop_fn(Tester, Tab, Max) ->
    fun(Next, Limit, ExpireAt) ->
        Now = erlang:system_time(millisecond),
        ?assert(ExpireAt =< Now - ?RETENTION),
        ?assertEqual(?SCAN_LIMIT, Limit),
        Tester ! {next, Tab, Next},
        N =
            case get({next, Tab}) of
                undefined ->
                    1;
                X ->
                    X + 1
            end,
        put({next, Tab}, N),
        case N > Max of
            true ->
                complete;
            false ->
                {continue, N}
        end
    end.
