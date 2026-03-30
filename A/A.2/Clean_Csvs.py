import csv
import os

INPUT_FOLDER = "A\\A.2\\RealdatafromXML_Csvs"      # folder with original CSVs
OUTPUT_FOLDER = "A\\A.2\\Cleaned_Csvs"    # folder for cleaned CSVs

os.makedirs(OUTPUT_FOLDER, exist_ok=True)


def clean_field(field):
    if field is None:
        return ""

    # Remove problematic newlines inside fields
    field = field.replace("\n", " ").replace("\r", " ")

    # Remove null bytes
    field = field.replace("\x00", "")

    # Trim whitespace
    field = field.strip()

    # Escape quotes properly for CSV
    field = field.replace('"', '""')

    # Optional: normalize DBLP author separator
    field = field.replace('|', ',')

    return field


def process_file(input_path, output_path):
    with open(input_path, "r", encoding="utf-8", errors="replace") as infile, \
         open(output_path, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.reader(infile, delimiter=';', quotechar='"')
        writer = csv.writer(outfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL)

        for line_number, line in enumerate(infile, 1):
            try:
                # Basic normalization
                line = line.strip()

                # Split using semicolon (DBLP-style)
                parts = line.split(';')

                # Clean each field
                cleaned_parts = [clean_field(p) for p in parts]

                writer.writerow(cleaned_parts)

            except Exception as e:
                print(f" Error in {input_path} at line {line_number}: {e}")


def main():
    for filename in os.listdir(INPUT_FOLDER):
        if filename.lower().endswith(".csv"):
            input_path = os.path.join(INPUT_FOLDER, filename)

            output_filename = filename.replace(".csv", "_clean.csv")
            output_path = os.path.join(OUTPUT_FOLDER, output_filename)

            print(f" Processing: {filename}")
            process_file(input_path, output_path)
            print(f" Saved: {output_filename}")

    print("\n All files processed.")


if __name__ == "__main__":
    main()