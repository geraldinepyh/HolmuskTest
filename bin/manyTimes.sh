#!/bin/bash
## if you configure the parameters correctly
## under argParsers, /lib/argParsers/module2 and addAllParsers.py (parsersAdd and decodeParsers)
## /lib/argParsers/module2 : define what parameters you can accept and how to decode (using config.py as a template.)
## addAllParsers.py : consolidate with the parameters for other modules 

python3 HolmuskTest.py --module2_value1 12
python3 HolmuskTest.py --module2_value1 14
python3 HolmuskTest.py --module2_value1 16
python3 HolmuskTest.py --module2_value1 18
python3 HolmuskTest.py --module2_value1 20
