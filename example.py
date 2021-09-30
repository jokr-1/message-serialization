from messaging import register, Message, deserialize_message

@register()
class MessageUser(Message):
    name = 'utf-8'
    age = '<B'
    hobbies = ['utf-8']


@register()
class MessageLarge(Message):
    timestamp = '<d'
    data = ['json']

@register(compress=True)
class MessageLargeCompressed(Message):
    timestamp = '<d'
    data = ['json']


# serialize user 
user_serialized = MessageUser.serialize(name='Gerhard', age=18, hobbies=['Fahrradfahren'])
print(user_serialized)

# create class instance of user 
user = deserialize_message(user_serialized)
print(user)

