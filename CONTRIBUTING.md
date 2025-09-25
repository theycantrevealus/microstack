# 🤝 Contributing to Microstack

Thanks for checking out **Microstack**! 🚀  
This project is meant to make it easier to run **Kafka in KRaft mode** with all the goodies (SCRAM auth, Schema Registry, Connect, Prometheus, etc.) using Docker Compose — but in a **lightweight, resource-friendly way**.  

We’d love for you to collaborate, improve, and share ideas!  

---

## 🛠 How to Contribute

### 1. Fork & Clone
First, fork this repo on GitHub, then clone your fork:
```bash
git clone https://github.com/<your-username>/microstack.git
cd microstack
```

### 2. Pick a Profile
Microstack supports multiple profiles for different use cases:
```bash
./start.sh core      # only brokers
./start.sh full      # full stack (Schema Registry, Connect, Prometheus, etc.)
```
This helps contributors test changes on both small and complete setups.

### 3. Make Changes
You can contribute in many ways:
1. Fix a bug
2. Add or improve healthchecks
3. Optimize Dockerfile or docker-compose.yml
4. Add documentation/tutorials
5. Suggest new profiles

👉 Anything that makes Kafka easier to run is welcome!

### 4. Test Your Changes
Make sure the stack still runs properly:
```bash
docker compose --profile full up -d
docker ps
```
Check services are healthy before submitting your PR.

### 5. Submit a Pull Request
Use a clear PR title with prefix (fix:, feat:, docs:, etc.)
Describe what you changed and why
Add screenshots or logs if useful

---

# 💡 Ideas for Contributions
If you’re not sure where to start, here are some ideas:
1. New profiles (e.g. observability, secure)
2. Add Grafana dashboards for Prometheus
3. Improve container startup ordering
4. Tune resource usage (CPU, memory)
5. Provide Kafka Connect connector examples
6. Add ksqlDB sample queries
7. Write or improve documentation

---

# 📬 Questions & Discussions
1. Open an Issue if you find a bug 🐞
2. Open a Discussion if you want to brainstorm 💭
No contribution is too small — even fixing typos in the docs helps a lot! 🙏

---

⚡ Happy hacking, and welcome to the Microstack community!
