import subprocess
import random
import string

def random_string(length=5):
    """Generate a random string of uppercase letters and digits."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def add_random_key_value_pairs():
    url = "http://127.0.0.1:5980/post"
    for _ in range(100):
        key = random_string()
        value = random_string()
        command = f'curl -X POST {url}/{key}/{value}'
        print(f"Executing: {command}")  # Print the command for reference
        subprocess.run(command, shell=True, check=True)

if __name__ == "__main__":
    add_random_key_value_pairs()