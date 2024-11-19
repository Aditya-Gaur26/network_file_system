import os

# Base directory
base_dir = 'myserver'
output_file = 'available_paths.txt'

# Open the output file
with open(output_file, 'w') as f:
    # Walk through all directories and files in base_dir
    for root, dirs, files in os.walk(base_dir):
        # Write each directory path with a trailing '/'
        for directory in dirs:
            dir_path = os.path.join(root, directory)
            # Get the path relative to base_dir
            relative_path = os.path.relpath(dir_path, base_dir)
            f.write(relative_path + '/' + '\n')
        
        # Write each file path without a trailing '/'
        for file in files:
            file_path = os.path.join(root, file)
            # Get the path relative to base_dir
            relative_path = os.path.relpath(file_path, base_dir)
            f.write(relative_path + '\n')

print(f"All paths saved to '{output_file}'")
