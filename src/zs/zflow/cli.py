import click
from zs.zflow.core import ZFlow
@click.command()
@click.argument("path", type=click.Path(exists=True))
def run(path):
    zflow = ZFlow(path)
    zflow.run()

if __name__ == "__main__":
    run()
