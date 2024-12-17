import os
import gzip
import shutil

# Define the input and output directory
input_directory = "data/transformed/"
output_directory = "data/transformed/uncompressed/"

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

def decompress_all_gz_files(input_dir, output_dir):
    """Decompress all .csv.gz files in a directory."""
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv.gz"):  # Process only .csv.gz files
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_dir, filename.replace(".gz", ""))  # Remove .gz extension

            # Decompress the file
            with gzip.open(input_file, 'rb') as f_in:
                with open(output_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            print(f"Decompressed: {input_file} -> {output_file}")

# Run the function
decompress_all_gz_files(input_directory, output_directory)