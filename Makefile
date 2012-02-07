REBAR=./rebar
PLT=.leader_cron_plt
DEPS=$(wildcard deps/*/ebin)

all:
	$(REBAR) compile

clean:
	$(REBAR) clean

deps:
	$(REBAR) get-deps

check:
	$(REBAR) eunit skip_deps=true

dialyzer: clean all
	dialyzer --plt $(PLT) -Wno_undefined_callbacks ebin

build_plt: all
	dialyzer --build_plt --output_plt $(PLT) \
		--apps erts kernel stdlib $(DEPS)