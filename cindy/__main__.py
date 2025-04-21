from datetime import date, datetime
import json
from pathlib import Path
import asyncio

import click


TAGS = [
    "SubSecCreateDate",
    "CreateDate",
    "SubSecDateTimeOriginal",
    "DateTimeOriginal",
    "SubSecModifyDate",
    "ModifyDate",
    "FileModifyDate",
]

DATETIME_FORMATS = [
    "%Y:%m:%d %H:%M:%S.%f%z",
    "%Y:%m:%d %H:%M:%S%z",
    "%Y:%m:%d %H:%M:%S.%f",
    "%Y:%m:%d %H:%M:%S",
    "%Y:%m:%d",
]

EXCLUDE = [
    ".DS_Store",
]


semaphore = asyncio.Semaphore(10)


@click.command()
@click.argument(
    "images_dir", type=click.Path(exists=True, path_type=Path), default=Path.cwd()
)
def main(images_dir: Path):
    asyncio.run(_main(images_dir, TAGS))


async def _main(images_dir: Path, tags: list[str]):
    results = await asyncio.gather(
        *[
            exiftool(image_path, tags=tags)
            for image_path in images_dir.iterdir()
            if image_path.name not in EXCLUDE
        ],
        return_exceptions=True,
    )

    for result in results:
        if isinstance(result, UnknownFileTypeError):
            click.echo(result, err=True)
        if isinstance(result, Exception):
            raise result
        else:
            path, tags = result
            tag, value = get_date(tags)
            click.echo(f"Analyzed {path}, using {tag}: {value}")

            path_new = path.parent / str(parse_date(value)) / path.name
            path_new.parent.mkdir(parents=True, exist_ok=True)

            click.echo(f"Moving {path} to {path_new}")
            path.rename(path_new)


def get_date(tags: dict[str, str]) -> tuple[str, str]:
    for tag in sorted(tags.keys(), key=TAGS.index):
        value = tags[tag]
        if value and not value.startswith("0000"):
            return tag, value
    raise ValueError("Could not find date")


def parse_date(value: str) -> date:
    for format in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, format).date()
        except ValueError:
            pass
    raise ValueError(f"Could not parse {value}")


class UnknownFileTypeError(RuntimeError):
    pass


async def exiftool(path: Path, tags: list[str] | None = None):
    click.echo(f"Reading {path}")
    cmd = ["exiftool", "-json"]
    if tags:
        cmd += [f"-{tag}" for tag in tags]
    cmd.extend(["--", str(path)])

    async with semaphore:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        code = await proc.wait()
        if code != 0:
            stderr_text = (
                stderr.decode().strip() or f"Error reading {path}, code: {code}"
            )
            if "Unknown file type" in stderr_text:
                raise UnknownFileTypeError(stderr_text)
            raise RuntimeError(stderr_text)

        tags = json.loads(stdout.decode())[0]
        del tags["SourceFile"]
        return (path, tags)


if __name__ == "__main__":
    main()
