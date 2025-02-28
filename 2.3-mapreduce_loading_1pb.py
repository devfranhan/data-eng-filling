# map 
 
import sys

def mapper():
    for line in sys.stdin:
        fields = line.strip().split(",")
        print(f"{fields[0]}\t1")  # Emite chave-valor

#reduce

def reducer():
    current_key = None
    current_count = 0

    for line in sys.stdin:
        key, value = line.strip().split("\t")
        if key != current_key:
            if current_key:
                print(f"{current_key}\t{current_count}")
            current_key = key
            current_count = 0
        current_count += int(value)
