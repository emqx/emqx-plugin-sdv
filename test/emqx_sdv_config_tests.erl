-module(emqx_sdv_config_tests).

-include_lib("eunit/include/eunit.hrl").

parse_test_() ->
    [
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => <<"10s">>})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => <<"10000">>})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => <<"10000ms">>})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => 9999.9})),
        ?_assertEqual(#{data_retention => 10000}, parse(#{<<"data_retention">> => 9999.1}))
    ].

parse(Config) ->
    emqx_sdv_config:parse(Config).
