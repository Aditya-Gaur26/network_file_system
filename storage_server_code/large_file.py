# Define the size of the file (1 GB = 1024 * 1024 * 1024 bytes)
file_size = 1 * 1024 * 1024 * 1024  # 1 GB

# Character to fill the file with
character = 'A'  # You can choose any character you want

# Open the output file in write mode
with open("large_file.txt", "w") as file:
    # Write enough characters to fill the file to the desired size
    # Since each character is 1 byte, write `file_size` number of characters
    file.write(character * file_size)

print("File of size 1 GB created successfully.")
