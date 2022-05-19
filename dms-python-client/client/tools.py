from io import TextIOWrapper


def output(file: TextIOWrapper | None, output, end='\n'):
    if file:
        try:
            file.write(str(output) + end)
        except:
            print(output, end=end)
    else:
        print(output, end=end)
