import re

input_string = "1gbps"

pattern = r'(\d+)(gb|mb)'

match = re.search(pattern, input_string)

value = int(match.group(1))

if match:
    unit = match.group(2)
    if unit == "gb":
        cleaned_value = value * 1000
    else:
        cleaned_value = value

print(cleaned_value)


def internet_speed_cleanup(string):
    pattern = r'(\d+)(gb|mb)'
    gb_match = re.search(pattern, string)
    value = int(gb_match.group(1))

    if gb_match:
        unit = gb_match.group(2)
        if unit == "gb":
            cleaned_value = value * 1000
        else:
            cleaned_value = value

    return cleaned_value


print(internet_speed_cleanup(input_string))
