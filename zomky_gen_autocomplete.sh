#!/bin/bash
java -cp "build/libs/zomky.jar" io.github.zomky.cli.ZomkyAutoComplete

# without extensions:
# alias zomky='java -jar build/libs/zomky.jar'
# java -cp "build/libs/zomky.jar" picocli.AutoComplete -n zomky --force io.github.zomky.cli.command.MainCommand
