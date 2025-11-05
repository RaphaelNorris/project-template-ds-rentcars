import os
import sys
import json
import requests
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime


class ExceptionHandler:
    @staticmethod
    def handle_exception(exception, context=""):
        print(f"Error in {context}: {str(exception)}")


class ReviewAgent:
    def __init__(self):
        load_dotenv()
        self.token = os.getenv("FLOW_TOKEN")
        if not self.token:
            raise Exception("❌ FLOW_TOKEN não encontrado no .env")

        self.base_url = "https://flow..com/channels-service"
        self.headers = {
            "Authorization": f"Bearer " + self.token,
            "Content-Type": "application/json"
        }

    def create_payload(self, message: str):
        return {
            "content": [{"type": "text/plain", "value": message}],
            "model": {
                "name": "claude37sonnet",
                "provider": "amazon-bedrock",
                "modelSettings": []
            },
            "agent": "code-review",
            "sources": [],
            "connectors": [],
            "operation": "new-question"
        }

    def analyze(self, code: str, filename: str) -> Optional[str]:
        try:
            prompt = self.build_prompt(code, filename)
            url1 = f"{self.base_url}/v2/chat/messages"
            resp1 = requests.post(url1, headers=self.headers, json=self.create_payload(prompt))

            if resp1.status_code != 200:
                ExceptionHandler.handle_exception(Exception(resp1.text), context="analyze/init")
                return None

            chat_id = json.loads(resp1.text.strip().split("\n")[0]).get("chatId")
            if not chat_id:
                return None

            url2 = f"{self.base_url}/v1/chat/{chat_id}/messages"
            resp2 = requests.get(url2, headers=self.headers)

            if resp2.status_code != 200:
                ExceptionHandler.handle_exception(Exception(resp2.status_code), context="analyze/fetch")
                return None

            result = ""
            for msg in resp2.json().get("messages", []):
                for item in msg.get("metadata", {}).get("content", []):
                    if item.get("author") == "assistant":
                        result += self.clean(item.get("value", ""))

            return result

        except Exception as e:
            ExceptionHandler.handle_exception(e, context="analyze/global")
            return None

    def build_prompt(self, content, filename):
        return f"""
                # system:
                Você é um Tech Lead especializado em Ciência de Dados. Seu papel é revisar scripts de Python (.py e .ipynb), avaliando qualidade técnica, boas práticas e legibilidade.

                # user:
                Revise o arquivo `{filename}` a seguir com atenção técnica. Aponte:

                1. Aderência a boas práticas de Python e PEP8
                2. Presença, clareza e padrão das docstrings (Google, NumPy, etc.)
                3. Organização, modularidade e estrutura
                4. Nomenclatura de variáveis e funções
                5. Uso adequado de bibliotecas de ciência de dados
                6. Sugestões para melhoria de performance, leitura e manutenção
                7. Pontos de alerta ou antipadrões

                Seu relatório deve conter:

                - Título do arquivo
                - Pontos positivos
                - Pontos de atenção
                - Sugestões de melhoria
                - Conclusão geral sobre a qualidade técnica do código

                Responda com um tom técnico, claro e direto. Não inclua blocos de código desnecessários.
                <arquivo>
                {content}
                </arquivo>
                        """

    def clean(self, text):
        import re
        return re.sub(r'```[\w]*\n?|```', '', text)


def process_review(source_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    agent = ReviewAgent()
    count = 0

    for root, _, files in os.walk(source_dir):
        for file in files:
            if not file.endswith(('.py', '.ipynb')):
                continue

            abs_path = os.path.join(root, file)
            try:
                with open(abs_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            except UnicodeDecodeError:
                with open(abs_path, 'r', encoding='latin-1') as f:
                    content = f.read()

            if not content.strip():
                continue

            print(f"🔍 Revisando: {file}")
            review = agent.analyze(content, file)
            if review:
                filename_base = os.path.splitext(file)[0]
                output_path = os.path.join(output_dir, f"{filename_base}_review.md")

                with open(output_path, 'w', encoding='utf-8') as out:
                    out.write(f"# Revisão Técnica: {file}\n")
                    out.write(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n")
                    out.write(review)

                print(f"✅ Salvo em: {output_path}")
                count += 1

    print(f"\n🧾 {count} arquivo(s) revisado(s) com sucesso.")
    print(f"📁 Relatórios salvos em: {output_dir}")


if __name__ == "__main__":
    print("📂 Caminho da pasta com os scripts (.py ou .ipynb):")
    src = input(">> ").strip()

    print("📁 Caminho da pasta onde deseja salvar os relatórios:")
    dest = input(">> ").strip()

    if not os.path.isdir(src):
        print(f"❌ Diretório inválido: {src}")
        sys.exit(1)

    process_review(src, dest)
