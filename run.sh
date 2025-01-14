#!/bin/sh

export ENV_ENUM_COLUMN=status

run_native(){
	export ENV_ENUM_STRING=unspecified
	cat sample.d/input.avsc | ./rs-avro-enum-str2num
}

run_wasmer(){
	cat sample.d/input.avsc |
		wasmer \
			run \
			--env ENV_ENUM_STRING=ok \
			--env ENV_ENUM_COLUMN=status \
			./rs-avro-enum-str2num.wasm
}

run_wasmtime(){
	cat sample.d/input.avsc |
		wasmtime \
			run \
			--env ENV_ENUM_STRING=ng \
			--env ENV_ENUM_COLUMN=status \
			./rs-avro-enum-str2num.wasm
}

run_wazero(){
	cat sample.d/input.avsc |
		wazero \
			run \
			-env ENV_ENUM_STRING=unspecified \
			-env ENV_ENUM_COLUMN=status \
			./rs-avro-enum-str2num.wasm
}

run_native
run_wasmer
run_wasmtime
run_wazero
