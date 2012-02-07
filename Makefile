REBAR=./rebar
PLT=.leader_cron_plt
DEPS=$(wildcard deps/*/ebin)

.PHONY: doc

all: get-deps
	$(REBAR) compile

clean: clean-doc
	$(REBAR) clean

get-deps:
	$(REBAR) get-deps

check:
	$(REBAR) eunit skip_deps=true

dialyzer: clean all
	dialyzer --plt $(PLT) -Wno_undefined_callbacks ebin

build_plt: all
	dialyzer --build_plt --output_plt $(PLT) \
		--apps erts kernel stdlib $(DEPS)

doc:
	$(REBAR) doc

clean-doc:
	rm -rf doc
