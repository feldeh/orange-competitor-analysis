import traceback


def divide_numbers(a, b):
    try:
        result = a / b
        return result
    except ZeroDivisionError as e:
        # Get the traceback information
        tb_info = traceback.format_exc()
        print(f"An error occurred in module '{__name__}' at line {traceback.tb_lineno}:")
        print(f"Error message: {e}")
        print("Traceback:")
        print(tb_info)


if __name__ == "__main__":
    a = 10
    b = 0
    result = divide_numbers(a, b)
    if result is not None:
        print(f"Result: {result}")
