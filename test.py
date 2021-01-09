import uuid

def printName(prefix):
    text = ''.join([prefix, str(uuid.uuid4())])
    return text

print(printName("test"))
