#!/bin/bash

docker run --rm -it \
  --name emqx \
  -p 1883:1883 \
  -p 18083:18083 \
  -v $(pwd)/_build/default/emqx_plugrel:/opt/emqx/plugins \
  -e EMQX_PLUGINS__STATES='[{enable = true, name_vsn = "emqx_sdv_fanout-1.0.0"}]' \
  emqx/emqx-enterprise:5.8.6 emqx console
