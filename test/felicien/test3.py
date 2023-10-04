import re


def internet_speed_cleanup(string):
    match = re.search(r'(\d+)\s*gb', string, re.IGNORECASE)

    if match:
        digits = int(match.group(1))
        return digits * 1000

    match = re.search(r'(\d+)', string)

    if match:
        return int(match.group(1))


print(internet_speed_cleanup('1 gb'))
print(internet_speed_cleanup('1 mb'))
