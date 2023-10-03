import re


def internet_speed_cleanup(string):
    pattern = r'(\d+)(gb)'
    match = re.search(pattern, string)
    value = int(match.group(1))

    if match:
        unit = match.group(2)
        if unit == "gb":
            cleaned_value = value * 1000
        else:
            cleaned_value = value

    return cleaned_value


print(internet_speed_cleanup('1gb'))
print(internet_speed_cleanup('1mb'))
