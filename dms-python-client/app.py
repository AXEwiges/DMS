import sys
from client.config import Config
from client.errors import SQLError
from client.executor import Executor
from client.interpreter import Interpreter
from argparse import ArgumentParser, BooleanOptionalAction

from client.tools import output


class Prompt:
    NEW = ">>> "
    CONTINUE = "... "


parser = ArgumentParser()
parser.add_argument('--batch', '-b', help='path to the batch file', default="")
parser.add_argument('--output', '-o', help='path to the output file', default="")
parser.add_argument('--echo', action=BooleanOptionalAction,
                    help='whether to echo statements in batch mode', default=True)
args = parser.parse_args()
if not args.batch and args.output:
    print('--output and --no-echo can only be set in batch mode.')
    sys.exit(1)


if args.batch:
    batch_file = open(args.batch, 'r')
else:
    print('Welcome to SQL!')
    print('Type .exit to quit.')

output_file = open(args.output, 'w') if args.output else None

interpreter = Interpreter()
config = Config()
executor = Executor(config.master_host, config.master_port, output_file)
interpreter.set_executor(executor)

prompt = Prompt.NEW

if args.batch:
    do_echo = args.echo
    try:
        assert batch_file
        for line in batch_file:
            if do_echo and line.strip():
                output(output_file, ">>> " + line, end="")
            interpreter.parse(line.strip())
    except SQLError as e:
        output(output_file, e)
    finally:
        batch_file.close()
        if args.output:
            output_file.close()
else:
    while True:
        line = input(prompt)
        if line == '.exit':
            break
        try:
            if interpreter.parse(line):
                prompt = Prompt.NEW
            else:
                prompt = Prompt.CONTINUE
        except SQLError as e:
            print(e)
            prompt = Prompt.NEW
