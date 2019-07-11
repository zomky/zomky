#!/bin/bash
java -cp "build/libs/zomky.jar" io.github.pmackowski.rsocket.raft.cli.ZomkyAutoComplete

# without extensions:
# alias zomky='java -jar build/libs/zomky.jar'
# java -cp "build/libs/zomky.jar" picocli.AutoComplete -n zomky --force io.github.pmackowski.rsocket.raft.cli.command.MainCommand
