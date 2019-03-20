import sqlite3
import io

class Connect(object):

    def __init__(self, db_name):
        try:
            self.conn = sqlite3.connect(db_name)
            self.cursor = self.conn.cursor()

            print("Banco:", db_name)

            self.cursor.execute('SELECT SQLITE_VERSION()')
            self.data = self.cursor.fetchone()
            print("SQLite version: %s" % self.data)
        except sqlite3.Error:
            print("Erro ao abrir banco.")
            return False

    def commit_db(self):
        if self.conn:
            self.conn.commit()

    def close_db(self):
        if self.conn:
            self.conn.close()
            print("Conexão fechada.")

class ClientesDb():

    tb_name = 'clientes'

    def __init__(self):
        self.db = Connect('clientes.db')
        self.tb_name

    def criar_schema(self, schema_name='sql/clientes_schema.sql'):
        print("Criando tabela %s ..." % self.tb_name)

        try:
            with open(schema_name, 'rt') as f:
                schema = f.read()
                self.db.cursor.executescript(schema)
        except sqlite3.Error:
            print("Aviso: A tabela %s já existe." % self.tb_name)
            return False

        print("Tabela %s criada com sucesso." % self.tb_name)