import base64

from argparse import ArgumentParser

from igor import config
from igor import domain
from igor import service


conf = config.read_default_config()
svc = service.IgorDBService(config=conf)

ps = ArgumentParser()
ps.add_argument("-n", "--name", default="user", help="username")
ps.add_argument("-p", "--password", default="password", help="password")
ps.add_argument("-a", "--is-admin", action="store_true", help="set user as admin")
args = ps.parse_args()

u = domain.User(name=args.name)
u.is_admin = args.is_admin
u.password = args.password

svc.create_user(u)

token = str(
    base64.b64encode(
        bytes(f"{u.name}:{args.password}", encoding='utf8')
    )
)

print(f"id: {u.id}")
print(f"username: {u.name}")
print(f"token: {token}")
