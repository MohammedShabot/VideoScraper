import argparse
from main import EmotionVideoDatasetBuilder


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the Emotion Video Dataset Builder"
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file containing emotions/subjects/settings"
    )

    parser.add_argument(
        "--output-dir",
        default="data/results",
        help="Directory to store output JSON files"
    )

    parser.add_argument(
        "--style",
        default="simple",
        choices=["simple", "expanded", "creative"],
        help="Query generation style"
    )

    parser.add_argument(
        "--platforms",
        nargs="+",
        default=["youtube"],
        help="Platforms to scrape (e.g. youtube vimeo)"
    )

    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Start scraping from query index (resume support)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Save intermediate results every N queries"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    builder = EmotionVideoDatasetBuilder(
        csv_path=args.csv,
        output_dir=args.output_dir
    )

    builder.run(
        style=args.style,
        platforms=args.platforms,
        start_from=args.start_from,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
