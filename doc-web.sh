#!/bin/sh

port=11480
docd=./target
host=127.0.0.1

miniserve \
	--port ${port} \
	--interfaces "${host}" \
	"${docd}"
