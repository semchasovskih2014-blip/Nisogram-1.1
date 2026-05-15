#!/usr/bin/env python3
"""
Мессенджер на Python с регистрацией и авторизацией
Поддерживает: личные сообщения, комнаты, онлайн-статус, историю сообщений
"""

import os
import sys
import json
import hashlib
import secrets
import time
import datetime
import threading
import socket
import select
from pathlib import Path

# ==================== КОНФИГУРАЦИЯ ====================
CONFIG = {
    'host': 'localhost',
    'port': 5555,
    'max_clients': 100,
    'buffer_size': 4096,
    'db_file': 'messenger_db.json',
    'history_dir': 'chat_history'
}

# ==================== БАЗА ДАННЫХ ====================
class Database:
    """Управление базой данных пользователей и сообщений"""
    
    def __init__(self, db_file):
        self.db_file = db_file
        self.data = self.load()
    
    def load(self):
        """Загрузка данных из файла"""
        if os.path.exists(self.db_file):
            with open(self.db_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {
            'users': {},
            'rooms': {
                'general': {
                    'name': 'general',
                    'created': datetime.datetime.now().isoformat(),
                    'members': []
                }
            },
            'messages': []
        }
    
    def save(self):
        """Сохранение данных в файл"""
        with open(self.db_file, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
    
    def register_user(self, username, password):
        """Регистрация нового пользователя"""
        if username in self.data['users']:
            return False, "Пользователь уже существует"
        
        if len(username) < 3 or len(username) > 20:
            return False, "Имя пользователя должно быть от 3 до 20 символов"
        
        if len(password) < 4:
            return False, "Пароль должен быть не менее 4 символов"
        
        self.data['users'][username] = {
            'password': hashlib.sha256(password.encode()).hexdigest(),
            'registered': datetime.datetime.now().isoformat(),
            'rooms': ['general'],
            'friends': []
        }
        self.save()
        return True, "Регистрация успешна"
    
    def authenticate(self, username, password):
        """Аутентификация пользователя"""
        if username in self.data['users']:
            hashed = hashlib.sha256(password.encode()).hexdigest()
            if self.data['users'][username]['password'] == hashed:
                return True, "Вход выполнен"
        return False, "Неверное имя пользователя или пароль"
    
    def get_user(self, username):
        """Получение информации о пользователе"""
        return self.data['users'].get(username)
    
    def add_message(self, from_user, to_user, message, room=None):
        """Сохранение сообщения"""
        msg = {
            'from': from_user,
            'to': to_user,
            'message': message,
            'room': room,
            'timestamp': datetime.datetime.now().isoformat(),
            'time': time.time()
        }
        self.data['messages'].append(msg)
        
        # Ограничиваем историю 10000 сообщений
        if len(self.data['messages']) > 10000:
            self.data['messages'] = self.data['messages'][-10000:]
        
        self.save()
        return msg
    
    def get_history(self, user1, user2, limit=50):
        """Получение истории переписки между пользователями"""
        history = []
        for msg in reversed(self.data['messages']):
            if len(history) >= limit:
                break
            if (msg['from'] == user1 and msg['to'] == user2) or \
               (msg['from'] == user2 and msg['to'] == user1):
                if not msg.get('room'):
                    history.insert(0, msg)
        return history
    
    def get_room_history(self, room, limit=50):
        """Получение истории сообщений в комнате"""
        history = []
        for msg in reversed(self.data['messages']):
            if len(history) >= limit:
                break
            if msg.get('room') == room:
                history.insert(0, msg)
        return history

# ==================== КЛИЕНТСКОЕ СОЕДИНЕНИЕ ====================
class Client:
    """Представление подключенного клиента"""
    
    def __init__(self, socket, address, username=None):
        self.socket = socket
        self.address = address
        self.username = username
        self.room = 'general'
        self.connected = True
        self.last_activity = time.time()
    
    def send(self, data):
        """Отправка данных клиенту"""
        try:
            self.socket.send((json.dumps(data) + '\n').encode('utf-8'))
            return True
        except:
            self.connected = False
            return False
    
    def receive(self):
        """Получение данных от клиента"""
        try:
            data = self.socket.recv(CONFIG['buffer_size']).decode('utf-8')
            if data:
                return json.loads(data.strip())
        except:
            pass
        return None
    
    def disconnect(self):
        """Отключение клиента"""
        self.connected = False
        try:
            self.socket.close()
        except:
            pass

# ==================== СЕРВЕР МЕССЕНДЖЕРА ====================
class MessengerServer:
    """Основной сервер мессенджера"""
    
    def __init__(self):
        self.db = Database(CONFIG['db_file'])
        self.clients = {}  # username -> Client
        self.sessions = {}  # session_token -> username
        self.running = True
        self.server_socket = None
        
    def start(self):
        """Запуск сервера"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((CONFIG['host'], CONFIG['port']))
            self.server_socket.listen(CONFIG['max_clients'])
            
            print("=" * 50)
            print("💬 МЕССЕНДЖЕР СЕРВЕР")
            print("=" * 50)
            print(f"📡 Хост: {CONFIG['host']}")
            print(f"🔌 Порт: {CONFIG['port']}")
            print("=" * 50)
            print("✨ Сервер готов к работе!\n")
            
            while self.running:
                # Принимаем новые подключения
                try:
                    client_socket, address = self.server_socket.accept()
                    client = Client(client_socket, address)
                    
                    # Запускаем поток для обработки клиента
                    thread = threading.Thread(target=self.handle_client, args=(client,))
                    thread.daemon = True
                    thread.start()
                    
                except Exception as e:
                    if self.running:
                        print(f"Ошибка при подключении: {e}")
                        
        except Exception as e:
            print(f"Ошибка запуска сервера: {e}")
        finally:
            self.stop()
    
    def stop(self):
        """Остановка сервера"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        
        # Отключаем всех клиентов
        for client in self.clients.values():
            client.disconnect()
        
        print("\n🛑 Сервер остановлен")
    
    def handle_client(self, client):
        """Обработка клиента"""
        try:
            while client.connected:
                # Ожидание команды от клиента
                data = client.receive()
                if data:
                    self.process_command(client, data)
                else:
                    break
                
        except Exception as e:
            print(f"Ошибка обработки клиента {client.address}: {e}")
        finally:
            self.remove_client(client)
    
    def process_command(self, client, data):
        """Обработка команд от клиента"""
        command = data.get('command')
        
        if command == 'register':
            self.cmd_register(client, data)
        elif command == 'login':
            self.cmd_login(client, data)
        elif command == 'logout':
            self.cmd_logout(client)
        elif command == 'message':
            self.cmd_message(client, data)
        elif command == 'room_message':
            self.cmd_room_message(client, data)
        elif command == 'join_room':
            self.cmd_join_room(client, data)
        elif command == 'create_room':
            self.cmd_create_room(client, data)
        elif command == 'get_users':
            self.cmd_get_users(client)
        elif command == 'get_history':
            self.cmd_get_history(client, data)
        elif command == 'get_rooms':
            self.cmd_get_rooms(client)
        elif command == 'private_message':
            self.cmd_private_message(client, data)
        else:
            client.send({'status': 'error', 'message': 'Неизвестная команда'})
    
    def cmd_register(self, client, data):
        """Регистрация нового пользователя"""
        username = data.get('username', '').strip()
        password = data.get('password', '')
        
        success, message = self.db.register_user(username, password)
        
        if success:
            client.send({
                'status': 'success',
                'command': 'register',
                'message': message
            })
        else:
            client.send({
                'status': 'error',
                'command': 'register',
                'message': message
            })
    
    def cmd_login(self, client, data):
        """Авторизация пользователя"""
        username = data.get('username', '').strip()
        password = data.get('password', '')
        
        # Проверка, не авторизован ли уже пользователь
        if username in self.clients:
            client.send({
                'status': 'error',
                'command': 'login',
                'message': 'Пользователь уже в сети'
            })
            return
        
        success, message = self.db.authenticate(username, password)
        
        if success:
            # Создаем сессию
            session_token = secrets.token_hex(32)
            client.username = username
            self.clients[username] = client
            self.sessions[session_token] = username
            
            # Отправляем подтверждение
            client.send({
                'status': 'success',
                'command': 'login',
                'message': message,
                'session_token': session_token,
                'username': username
            })
            
            # Уведомляем всех о входе пользователя
            self.broadcast({
                'type': 'user_joined',
                'username': username,
                'message': f"{username} присоединился к чату"
            })
            
            print(f"✅ {username} подключился из {client.address}")
        else:
            client.send({
                'status': 'error',
                'command': 'login',
                'message': message
            })
    
    def cmd_logout(self, client):
        """Выход из системы"""
        if client.username:
            username = client.username
            self.remove_client(client)
            
            # Уведомляем всех о выходе
            self.broadcast({
                'type': 'user_left',
                'username': username,
                'message': f"{username} покинул чат"
            })
            
            print(f"👋 {username} отключился")
    
    def cmd_message(self, client, data):
        """Обработка сообщения в комнате"""
        if not client.username:
            return
        
        message = data.get('message', '')
        room = data.get('room', 'general')
        
        if message:
            # Сохраняем сообщение
            self.db.add_message(client.username, 'room', message, room)
            
            # Отправляем сообщение всем в комнате
            msg_data = {
                'type': 'message',
                'from': client.username,
                'message': message,
                'room': room,
                'timestamp': datetime.datetime.now().strftime("%H:%M:%S")
            }
            
            self.broadcast_to_room(room, msg_data, exclude=client.username)
            client.send(msg_data)  # Отправляем и отправителю
    
    def cmd_room_message(self, client, data):
        """Отправка сообщения в комнату"""
        self.cmd_message(client, data)
    
    def cmd_private_message(self, client, data):
        """Отправка личного сообщения"""
        if not client.username:
            return
        
        to_user = data.get('to', '')
        message = data.get('message', '')
        
        if message and to_user:
            # Сохраняем сообщение
            self.db.add_message(client.username, to_user, message)
            
            msg_data = {
                'type': 'private_message',
                'from': client.username,
                'message': message,
                'to': to_user,
                'timestamp': datetime.datetime.now().strftime("%H:%M:%S")
            }
            
            # Отправляем получателю, если он онлайн
            if to_user in self.clients:
                self.clients[to_user].send(msg_data)
            
            # Отправляем отправителю
            client.send(msg_data)
    
    def cmd_join_room(self, client, data):
        """Присоединение к комнате"""
        if not client.username:
            return
        
        room = data.get('room', '')
        
        if room in self.db.data['rooms']:
            old_room = client.room
            client.room = room
            
            # Добавляем пользователя в комнату если его там нет
            if client.username not in self.db.data['rooms'][room].get('members', []):
                self.db.data['rooms'][room].setdefault('members', []).append(client.username)
                self.db.save()
            
            client.send({
                'status': 'success',
                'command': 'join_room',
                'room': room,
                'message': f"Вы присоединились к комнате {room}"
            })
            
            # Уведомляем комнату
            self.broadcast_to_room(room, {
                'type': 'system',
                'message': f"{client.username} присоединился к комнате",
                'room': room
            })
        else:
            client.send({
                'status': 'error',
                'command': 'join_room',
                'message': 'Комната не найдена'
            })
    
    def cmd_create_room(self, client, data):
        """Создание новой комнаты"""
        if not client.username:
            return
        
        room_name = data.get('room', '').strip()
        
        if not room_name:
            client.send({'status': 'error', 'message': 'Неверное имя комнаты'})
            return
        
        if room_name in self.db.data['rooms']:
            client.send({'status': 'error', 'message': 'Комната уже существует'})
            return
        
        self.db.data['rooms'][room_name] = {
            'name': room_name,
            'created': datetime.datetime.now().isoformat(),
            'creator': client.username,
            'members': [client.username]
        }
        self.db.save()
        
        client.send({
            'status': 'success',
            'command': 'create_room',
            'room': room_name,
            'message': f"Комната {room_name} создана"
        })
    
    def cmd_get_users(self, client):
        """Получение списка онлайн пользователей"""
        if not client.username:
            return
        
        online_users = list(self.clients.keys())
        all_users = list(self.db.data['users'].keys())
        
        client.send({
            'status': 'success',
            'command': 'get_users',
            'online_users': online_users,
            'all_users': all_users
        })
    
    def cmd_get_history(self, client, data):
        """Получение истории сообщений"""
        if not client.username:
            return
        
        with_user = data.get('with_user')
        room = data.get('room')
        limit = data.get('limit', 50)
        
        if with_user:
            history = self.db.get_history(client.username, with_user, limit)
            client.send({
                'status': 'success',
                'command': 'get_history',
                'history': history,
                'with_user': with_user
            })
        elif room:
            history = self.db.get_room_history(room, limit)
            client.send({
                'status': 'success',
                'command': 'get_history',
                'history': history,
                'room': room
            })
    
    def cmd_get_rooms(self, client):
        """Получение списка комнат"""
        if not client.username:
            return
        
        rooms = list(self.db.data['rooms'].keys())
        client.send({
            'status': 'success',
            'command': 'get_rooms',
            'rooms': rooms
        })
    
    def broadcast(self, data, exclude=None):
        """Отправка сообщения всем клиентам"""
        for username, client in self.clients.items():
            if username != exclude:
                client.send(data)
    
    def broadcast_to_room(self, room, data, exclude=None):
        """Отправка сообщения всем в комнате"""
        for username, client in self.clients.items():
            if username != exclude and client.room == room:
                client.send(data)
    
    def remove_client(self, client):
        """Удаление клиента из списка"""
        if client.username and client.username in self.clients:
            del self.clients[client.username]
        
        # Удаляем сессии
        to_remove = []
        for token, username in self.sessions.items():
            if username == client.username:
                to_remove.append(token)
        
        for token in to_remove:
            del self.sessions[token]
        
        client.disconnect()

# ==================== КЛИЕНТ МЕССЕНДЖЕРА ====================
class MessengerClient:
    """Клиент мессенджера с консольным интерфейсом"""
    
    def __init__(self):
        self.socket = None
        self.username = None
        self.session_token = None
        self.running = True
        self.current_room = 'general'
        self.receive_thread = None
    
    def connect(self, host='localhost', port=5555):
        """Подключение к серверу"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((host, port))
            print("✅ Подключение к серверу установлено")
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
            return False
    
    def send_command(self, command):
        """Отправка команды на сервер"""
        try:
            self.socket.send((json.dumps(command) + '\n').encode('utf-8'))
            return True
        except:
            return False
    
    def receive_messages(self):
        """Поток для получения сообщений от сервера"""
        buffer = ""
        while self.running:
            try:
                data = self.socket.recv(CONFIG['buffer_size']).decode('utf-8')
                if data:
                    buffer += data
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        if line.strip():
                            self.handle_message(json.loads(line))
                else:
                    break
            except:
                break
        
        print("\n🔌 Соединение с сервером потеряно")
        self.running = False
    
    def handle_message(self, data):
        """Обработка входящих сообщений"""
        msg_type = data.get('type')
        
        if msg_type == 'message':
            # Сообщение в комнате
            print(f"\n[{data['timestamp']}] {data['from']}: {data['message']}")
            print("> ", end='', flush=True)
        
        elif msg_type == 'private_message':
            # Личное сообщение
            print(f"\n💬 [{data['timestamp']}] {data['from']} (личное): {data['message']}")
            print("> ", end='', flush=True)
        
        elif msg_type == 'user_joined':
            print(f"\n✨ {data['message']}")
            print("> ", end='', flush=True)
        
        elif msg_type == 'user_left':
            print(f"\n👋 {data['message']}")
            print("> ", end='', flush=True)
        
        elif msg_type == 'system':
            print(f"\n📢 {data['message']}")
            print("> ", end='', flush=True)
        
        elif data.get('status') == 'success':
            if data.get('command') == 'login':
                self.username = data['username']
                self.session_token = data['session_token']
                print(f"\n✅ {data['message']}")
                print("\n💡 Команды:")
                print("  /msg <user> <text> - личное сообщение")
                print("  /join <room> - присоединиться к комнате")
                print("  /create <room> - создать комнату")
                print("  /users - список пользователей")
                print("  /rooms - список комнат")
                print("  /history [user|room] - история сообщений")
                print("  /quit - выход\n")
                print(f"Текущая комната: {self.current_room}")
                print("> ", end='', flush=True)
            
            elif data.get('command') == 'register':
                print(f"\n✅ {data['message']}")
            
            elif data.get('command') == 'get_users':
                print("\n📋 ПОЛЬЗОВАТЕЛИ:")
                print(f"  Онлайн ({len(data['online_users'])}): {', '.join(data['online_users']) if data['online_users'] else 'нет'}")
                print(f"  Всего ({len(data['all_users'])}): {', '.join(data['all_users'])}")
            
            elif data.get('command') == 'get_rooms':
                print("\n🏠 КОМНАТЫ:")
                for room in data['rooms']:
                    print(f"  • {room}")
            
            elif data.get('command') == 'get_history':
                print(f"\n📜 ИСТОРИЯ СООБЩЕНИЙ:")
                if data.get('with_user'):
                    print(f"  с {data['with_user']}:")
                elif data.get('room'):
                    print(f"  в комнате {data['room']}:")
                
                for msg in data['history']:
                    time_str = msg['timestamp'].split('T')[1][:5] if 'T' in msg['timestamp'] else msg['timestamp']
                    print(f"  [{time_str}] {msg['from']}: {msg['message']}")
            
            elif data.get('command') == 'join_room':
                self.current_room = data['room']
                print(f"\n✅ {data['message']}")
        
        elif data.get('status') == 'error':
            print(f"\n❌ {data.get('message', 'Ошибка')}")
        
        if not msg_type and data.get('status') != 'success' and data.get('command') != 'login':
            print("> ", end='', flush=True)
    
    def register(self):
        """Регистрация нового пользователя"""
        print("\n📝 РЕГИСТРАЦИЯ")
        username = input("Имя пользователя: ").strip()
        password = input("Пароль: ").strip()
        confirm = input("Подтвердите пароль: ").strip()
        
        if password != confirm:
            print("❌ Пароли не совпадают")
            return False
        
        return self.send_command({
            'command': 'register',
            'username': username,
            'password': password
        })
    
    def login(self):
        """Вход в систему"""
        print("\n🔐 ВХОД В СИСТЕМУ")
        username = input("Имя пользователя: ").strip()
        password = input("Пароль: ").strip()
        
        return self.send_command({
            'command': 'login',
            'username': username,
            'password': password
        })
    
    def send_message(self, text):
        """Отправка сообщения"""
        if text.startswith('/'):
            self.handle_command(text)
        else:
            # Отправляем в текущую комнату
            self.send_command({
                'command': 'room_message',
                'message': text,
                'room': self.current_room
            })
    
    def handle_command(self, text):
        """Обработка команд"""
        parts = text.split(' ', 2)
        cmd = parts[0].lower()
        
        if cmd == '/msg' and len(parts) >= 3:
            # Личное сообщение
            to_user = parts[1]
            message = parts[2]
            self.send_command({
                'command': 'private_message',
                'to': to_user,
                'message': message
            })
        
        elif cmd == '/join' and len(parts) >= 2:
            # Присоединиться к комнате
            room = parts[1]
            self.send_command({
                'command': 'join_room',
                'room': room
            })
        
        elif cmd == '/create' and len(parts) >= 2:
            # Создать комнату
            room = parts[1]
            self.send_command({
                'command': 'create_room',
                'room': room
            })
        
        elif cmd == '/users':
            self.send_command({'command': 'get_users'})
        
        elif cmd == '/rooms':
            self.send_command({'command': 'get_rooms'})
        
        elif cmd == '/history':
            if len(parts) >= 2:
                target = parts[1]
                if target.startswith('@'):
                    self.send_command({
                        'command': 'get_history',
                        'with_user': target[1:]
                    })
                else:
                    self.send_command({
                        'command': 'get_history',
                        'room': target
                    })
            else:
                print("Использование: /history @user или /history room")
        
        elif cmd == '/quit':
            self.running = False
        
        else:
            print("Неизвестная команда. Доступные команды:")
            print("  /msg <user> <text> - личное сообщение")
            print("  /join <room> - присоединиться к комнате")
            print("  /create <room> - создать комнату")
            print("  /users - список пользователей")
            print("  /rooms - список комнат")
            print("  /history @user - история с пользователем")
            print("  /history room - история в комнате")
            print("  /quit - выход")
    
    def run(self):
        """Запуск клиента"""
        print("=" * 50)
        print("💬 МЕССЕНДЖЕР")
        print("=" * 50)
        
        if not self.connect():
            return
        
        while True:
            print("\n1. Вход")
            print("2. Регистрация")
            print("3. Выход")
            
            choice = input("\nВыберите действие: ").strip()
            
            if choice == '1':
                if self.login():
                    break
            elif choice == '2':
                if self.register():
                    print("✅ Регистрация успешна! Теперь войдите.")
            elif choice == '3':
                return
        
        # Запускаем поток для приема сообщений
        self.receive_thread = threading.Thread(target=self.receive_messages)
        self.receive_thread.daemon = True
        self.receive_thread.start()
        
        print("\n💬 Вы в чате! Введите сообщение или команду /help")
        
        # Основной цикл ввода
        while self.running:
            try:
                message = input("> ")
                if message.strip():
                    self.send_message(message.strip())
            except KeyboardInterrupt:
                print("\n")
                break
            except EOFError:
                break
        
        # Отправляем logout
        if self.username:
            self.send_command({'command': 'logout'})
        
        self.socket.close()
        print("\n👋 До свидания!")

# ==================== ЗАПУСК ====================
def main():
    """Главная функция"""
    print("\n" + "=" * 50)
    print("💬 МЕССЕНДЖЕР НА PYTHON")
    print("=" * 50)
    print("\n1. Запустить сервер")
    print("2. Запустить клиент")
    print("3. Выход")
    
    choice = input("\nВыберите действие: ").strip()
    
    if choice == '1':
        server = MessengerServer()
        try:
            server.start()
        except KeyboardInterrupt:
            print("\n🛑 Остановка сервера...")
            server.stop()
    
    elif choice == '2':
        client = MessengerClient()
        client.run()
    
    else:
        print("До свидания!")

if __name__ == "__main__":
    main()