from database import init_db, connect_db
from harvesters import UniHalleHarvester


def main():
    init_db()

    con = connect_db()
    cur = con.cursor()

    harvester = UniHalleHarvester()
    harvester.process(cur, con)

    con.close()
    print("\n✅ Repo 16 pipeline finished")


if __name__ == "__main__":
    main()