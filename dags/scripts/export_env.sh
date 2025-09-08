#!/bin/bash

# Export .env variable
export $(grep -v '^#' .env | xargs -d '\n')

exit 0