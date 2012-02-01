REBAR=./rebar

all:
	$(REBAR) compile

clean:
	$(REBAR) clean

deps:
	$(REBAR) get-deps

check:
	$(REBAR) eunit skip_deps=true