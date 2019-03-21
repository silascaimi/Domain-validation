import sqlite3
import io

class Connect():

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

    def inserir_email(self):
        self.email = input('Email: ')

        try:
            self.db.cursor.execute("""INSERT INTO clientes (email) VALUES (?)""", (self.email,))
            self.db.commit_db()
            print("Dados inseridos com sucesso.")
        except sqlite3.IntegrityError:
            print("Aviso: O email deve ser único.")
            return False

    def ler_todos_clientes(self):
        sql = 'SELECT * FROM clientes ORDER BY email'
        r = self.db.cursor.execute(sql)
        return r.fetchall()

    def imprimir_todos_clientes(self):
        lista = self.ler_todos_clientes()
        print('{:>3s} {:21s}'.format('id', 'email'))
        for c in lista:
            print('{:3d} {:>25s}'.format(c[0], c[1]))

    def localizar_cliente(self, email):
        r = self.db.cursor.execute('SELECT * FROM clientes WHERE email = ?', (email,))
        return r.fetchone()

    def imprimir_cliente(self):
        self.email = input("Email: ")
        if self.localizar_cliente(self.email) == None:
            print('Não existe cliente com o email informado.')
        else:
            print(self.localizar_cliente(self.email))

    def contar_cliente(self):
        r = self.db.cursor.execute('SELECT COUNT(*) FROM clientes')
        print("Total de clientes:", r.fetchone()[0])

    def deletar(self):
        self.email = input("Email: ")
        try:
            c = self.localizar_cliente(self.email)
            if c:
                self.db.cursor.execute("""DELETE FROM clientes WHERE email = ?""", (self.email,))
                self.db.commit_db()
                print("Registro %s excluído com sucesso." % self.email)
            else:
                print('Não existe cliente com o email informado.')
        except e:
            raise e

    def fechar_conexao(self):
        self.db.close_db()

c = ClientesDb()
c.criar_schema()
opcao = -1
while opcao != 0:
    print("\n************ Domain Validation **************\n")

    print("\tMENU\n")

    print("[1] - Inserir registro ")
    print("[2] - Imprimir todos" )
    print("[3] - Localizar")
    print("[4] - Total de registros")
    print("[5] - Deletar registro\n")

    print("[0] - Sair\n")

    opcao = int(input("Digite um número: "))

    if opcao == 1:
        c.inserir_email()
    elif opcao == 2:
        c.imprimir_todos_clientes()
    elif opcao == 3:
        c.imprimir_cliente()
    elif opcao == 4:
        c.contar_cliente()
    elif opcao == 5:
        c.deletar()

c.fechar_conexao()