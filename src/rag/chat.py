#rode no terminal: python3 -m src.rag.chat

import os
from src.rag.answer import answer
from dotenv import load_dotenv
load_dotenv()

def main():
    print("=== RAG ANEEL - Modo Interativo ===")
    print("Digite 'sair' para encerrar.\n")

    while True:
        question = input("Pergunta: ").strip()

        if question.lower() in ["sair", "exit", "quit"]:
            print("Encerrando.")
            break

        if not question:
            continue

        print("\n---\n")
        answer(question=question, top_k=3, model="gpt-5.5", use_llm=bool(os.getenv("OPENAI_API_KEY")))
        print("\n============================\n")

if __name__ == "__main__":
    main()