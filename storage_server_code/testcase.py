import os
import random

# Base directory for your server
base_dir = 'myserver'

# Configuration: Number of directories, files, and nested levels
num_directories = 5  # Top-level directories
num_files_per_directory = 8  # Files per top-level directory
num_nested_directories = 3  # Number of nested subdirectories
num_files_per_nested_directory = 4  # Files per nested directory

# Function to create files with varying content
def create_file_with_content(file_path, content_repetitions=4):
    with open(file_path, 'w') as file:
        content = (os.path.basename(file_path) + '\n') * content_repetitions
        file.write(content)

# Main script to create the directory and file structure
for i in range(num_directories):
    # Top-level directory path
    dir_path = os.path.join(base_dir, f'dir_{i}')
    os.makedirs(dir_path, exist_ok=True)

    # Create files in the top-level directory
    for j in range(num_files_per_directory):
        file_name = f'file_{j}.txt'
        file_path = os.path.join(dir_path, file_name)
        # Write the file name multiple times, with some files having varied content
        repetitions = random.randint(2, 5)
        create_file_with_content(file_path, repetitions)

    # Create nested directories with files
    for k in range(num_nested_directories):
        nested_dir_path = os.path.join(dir_path, f'nested_dir_{k}')
        os.makedirs(nested_dir_path, exist_ok=True)

        # Create files in the nested directory
        for l in range(num_files_per_nested_directory):
            nested_file_name = f'nested_file_{l}.txt'
            nested_file_path = os.path.join(nested_dir_path, nested_file_name)
            repetitions = random.randint(3, 6)
            create_file_with_content(nested_file_path, repetitions)

    # Add an empty file to each top-level directory
    empty_file_path = os.path.join(dir_path, 'empty_file.txt')
    open(empty_file_path, 'w').close()

print(f"Created {num_directories} top-level directories with nested structures in '{base_dir}'")
