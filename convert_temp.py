@outputSchema("fahrenheit:double")
def to_fahrenheit(celsius):
    return celsius * 9.0 / 5.0 + 32
