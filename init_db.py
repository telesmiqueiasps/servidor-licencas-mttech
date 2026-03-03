#!/usr/bin/env python3
"""Executado pelo Render no build — garante que o banco existe."""
import database as db
db.init()
print("Banco inicializado com sucesso.")
