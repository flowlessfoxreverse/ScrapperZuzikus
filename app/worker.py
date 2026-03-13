from dramatiq.cli import main as dramatiq_main


def main() -> None:
    raise SystemExit(dramatiq_main(["dramatiq", "app.tasks"]))


if __name__ == "__main__":
    main()
