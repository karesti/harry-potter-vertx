#!/usr/bin/env bash

kill `jps | grep DeathEater | cut -d ' ' -f 1`
kill `jps | grep DarkLord | cut -d ' ' -f 1`
kill `jps | grep Wizard | cut -d ' ' -f 1`