import sys

args = sys.argv[1:]

if args and args[0] == "migrate":
    from de_platform.cli.migrate import run_migrate

    sys.exit(run_migrate(args[1:]))
else:
    from de_platform.cli.runner import run_cli

    run_cli()
