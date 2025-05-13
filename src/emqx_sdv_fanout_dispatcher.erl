-module(emqx_sdv_fanout_dispatcher).

-export([batch/1, ack/2]).

-export([start_link/2]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_sdv_fanout.hrl").

%% @doc Handle a batch received from SDV platform.
batch(Payload) ->
    try emqx_utils_json:decode(Payload, [return_maps]) of
        #{<<"ids">> := VINs, <<"request_id">> := RequestId, <<"data">> := Data} ->
            ok = insert_batch(VINs, RequestId, Data),
            ok = notify_dispatchers(VINs, RequestId),
            ok;
        _ ->
            {error, invalid_payload}
    catch
        _:_ ->
            {error, invalid_payload}
    end.

%% @doc Handle an ACK received from a vehicle.
ack(VIN, RequestId) ->
    %% TODO: store the ACK, and send sync notification to dispatcher workers
    ?LOG(warning, "received_ack", #{vin => VIN, request_id => RequestId}),
    ok.

insert_batch(VINs, RequestId, Data) ->
    Now = now_ts(),
    {ok, ID} = emqx_sdv_fanout_data:insert(Now, Data),
    lists:foreach(fun(VIN) -> emqx_sdv_fanout_ids:insert(VIN, RequestId, Now, ID) end, VINs).

notify_dispatchers(_VINs, _RequestId) ->
    %% TODO: send sync notification to dispatcher workers
    ok.

now_ts() ->
    erlang:system_time(millisecond).

start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
