#!/bin/bash

ps ax | grep luigi | cut -c 1-6 | xargs kill
