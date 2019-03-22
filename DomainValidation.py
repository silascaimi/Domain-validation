import sqlite3
import io
import csv
import re
import os

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
        os.system('cls')
        if self.validar_email(self.email):
            if self.validar_dominio(self.email):
                try:
                    self.db.cursor.execute("""INSERT INTO clientes (email) VALUES (?)""", (self.email,))
                    self.db.commit_db()
                    print("Dados inseridos com sucesso.")
                except sqlite3.IntegrityError:
                    print("Aviso: O email deve ser único.")
                    return False
            else:
                print("Dominio inválido")
        else:
            print("Formato de email inválido")

    def inserir_de_csv(self, file_name='csv/email_list.csv'):
        os.system('cls')
        emails_validados = open('csv/emails_validados.csv', 'w')
        emails_invalidos = open('csv/emails_invalios.csv', 'w')
        cont_validos = 0
        cont_invalidos = 0
        cont_duplicados = 0
        cont_dominio_invalido = 0
        cont_email_sintaxe_error = 0
        email_list = []
        reader = csv.reader(open(file_name, 'rt'), delimiter=',')
        
        for (email,) in reader:
            email_list.append(email[1:-1])
        email_list = self.remove_repetidos(email_list)

        for email in email_list:
            if self.validar_email(email):
                if self.validar_dominio(email):
                    try:
                        self.db.cursor.execute("""INSERT INTO clientes (email) VALUES (?)""", (email,))
                    except sqlite3.IntegrityError:
                        cont_duplicados += 1
                    email += '\n'
                    emails_validados.write(email)
                    cont_validos += 1
                else:
                    email += '\n'
                    emails_invalidos.write(email)
                    cont_invalidos += 1
                    cont_dominio_invalido += 1
            else:
                email += '\n'
                emails_invalidos.write(email)
                cont_invalidos += 1
                cont_email_sintaxe_error +=1
        self.db.commit_db()

        print("Dados importados do csv com sucesso.\n")
        total_emails = cont_validos + cont_invalidos
        print("Emails verificados: {}".format(total_emails))
        print("Emails válidos: {} {}%".format(cont_validos, round(((cont_validos)/total_emails)*100, 2)))
        print("Emails inválidos: {} {}%\n".format(cont_invalidos, round(((cont_invalidos)/total_emails)*100, 2)))
        print("Emails com sintaxe incorreta: {}".format(cont_email_sintaxe_error))
        print("Emails com dominio invalido: {}\n".format(cont_dominio_invalido))
        print("Emails já existentes no banco: {}".format(cont_duplicados))
        print("Emails adicionados ao banco: {}".format(cont_validos-cont_duplicados))
        print("\nRelação de emails válidos criado em csv/emails_validados.csv")
        print("Relação de emails inválidos criado em csv/emails_invalios.csv\n")

    def ler_todos_clientes(self):
        sql = 'SELECT * FROM clientes ORDER BY email'
        r = self.db.cursor.execute(sql)
        return r.fetchall()

    def imprimir_todos_clientes(self):
        os.system('cls')
        lista = self.ler_todos_clientes()
        print('{:>3s} {:>21s}'.format('id', 'email'))
        for c in lista:
            print('{:3d} {:>35s}'.format(c[0], c[1]))

    def localizar_cliente(self, email):
        r = self.db.cursor.execute('SELECT * FROM clientes WHERE email = ?', (email,))
        return r.fetchone()

    def imprimir_cliente(self):
        self.email = input("Email: ")
        os.system('cls')
        if self.localizar_cliente(self.email) == None:
            print('Não existe cliente com o email informado.')
        else:
            print(self.localizar_cliente(self.email))

    def contar_cliente(self):
        os.system('cls')
        r = self.db.cursor.execute('SELECT COUNT(*) FROM clientes')
        print("Total de clientes:", r.fetchone()[0])

    def deletar(self):
        self.email = input("Email: ")
        os.system('cls')
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

    def validar_email(self, email):
        if re.match("^.+@(\[?)[a-zA-Z0-9-.]+.([a-zA-Z]{2,3}|[0-9]{1,3})(]?)$", email) != None:
            return True
        return False

    def validar_dominio(self, email):
        self.file_name='csv/domain_list.csv'
        dominios = csv.reader(open(self.file_name, 'rt'), delimiter=',')
        dominio = (dominios,)
        for dominio in dominios:
            dominio = dominio[0][1:-1]
            if dominio.lower() in email.lower():
                return True
        return False

    def remove_repetidos(self, lista):
        self.l = []
        self.repetidos = 0
        for i in lista:
            if i not in self.l:
                self.l.append(i)
            else:
                self.repetidos +=1
        #self.l.sort()
        print("Emails repetidos: {}".format(self.repetidos))
        return self.l

    def fechar_conexao(self):
        self.db.close_db()

c = ClientesDb()
c.criar_schema()

opcao = -1
while opcao != 0:
    print("\n************ Domain Validation **************\n")

    print("\tMENU\n")

    print("[1] - Inserir um registro")
    print("[2] - Inserir de arquivo CSV")
    print("[3] - Imprimir todos" )
    print("[4] - Localizar")
    print("[5] - Total de registros")
    print("[6] - Deletar registro\n")

    print("[0] - Sair\n")

    opcao = int(input("Digite um número: "))

    if opcao == 1:
        c.inserir_email()
    elif opcao == 2:
        c.inserir_de_csv()
    elif opcao == 3:
        c.imprimir_todos_clientes()
    elif opcao == 4:
        c.imprimir_cliente()
    elif opcao == 5:
        c.contar_cliente()
    elif opcao == 6:
        c.deletar()

c.fechar_conexao()

