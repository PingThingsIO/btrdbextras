import os
from pprint import pprint
import random
from btrdbextras.eventproc import hooks, register, deregister, list_handlers


print("HOOKS")
hks = hooks()
print(hks)


print("\n\nLIST HANDLERS")
pprint(list_handlers("ctingress.on_complete"))


print("\n\nREGISTER")
@register("foo-{}".format(random.randint(0,100)), hks[0], os.environ["BTRDB_API_KEY"], "success@example.com","failure@example.com", ["red", "blue"])
def transform(*args, **kwargs):
    print("i shouldnt run unless by the executor")
    print(args, kwargs)



print("\n\nLIST HANDLERS")
handlers = list_handlers()
pprint(handlers)

input()

print("\n\nDELETE HANDLERS")
for h in handlers:
    print("deleting: {}".format(h.id))
    deregister(h.id)



print("\n\nLIST HANDLERS")
pprint(list_handlers())




