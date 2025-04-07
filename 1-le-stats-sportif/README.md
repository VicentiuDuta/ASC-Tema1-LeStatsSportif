**Nume:** Duță Vicențiu-Alecsandru  
**Grupă:** 331CC  

# Tema 1 - Le Stats Sportif

---

## Organizare

### Explicație pentru soluția aleasă

Pentru a implementa serverul web care gestionează cereri statistice, am utilizat o arhitectură bazată pe trei componente principale:

1. **Webserver Flask** – Gestionează rutele API și direcționează cererile către modulele specifice de procesare a datelor  
2. **Thread Pool** – Sistem de procesare asincronă a job-urilor, implementat prin mecanisme de sincronizare  
3. **Data Ingestor** – Modul responsabil cu încărcarea și procesarea datelor din CSV și calculul diferitelor statistici  

**Abordarea generală** a fost să implementez un sistem care:
- Procesează datele din CSV la pornirea serverului
- Gestionează cereri simultane folosind un thread pool
- Utilizează un mecanism de job-uri care permite urmărirea progresului
- Salvează rezultatele procesării pe disc (simulând o bază de date)

**Fluxul general al unei cereri:**
1. Clientul face o cerere către server
2. Se atribuie un ID unic cererii (`job_id`)
3. Se creează un closure care încapsulează task-ul
4. Job-ul este adăugat în coada thread pool-ului
5. Se returnează `job_id` către client
6. Un worker preia job-ul, îl procesează și salvează rezultatul
7. Clientul poate interoga `/api/get_results/<job_id>` pentru a vedea rezultatul

### Sincronizare și concurență

Pentru a gestiona accesul concurent la resurse partajate:
- `Lock` pentru contorul de job-uri (`remaining_jobs_lock`)
- `Queue` sincronizată pentru distribuirea job-urilor
- `Event` pentru shutdown controlat

---

## Exemplu de cod pentru procesarea unui request

```python
# routes.py
@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    data = request.json
    webserver.logger.info("Received states_mean request with data: %s", data)
    if not webserver.tasks_runner.graceful_shutdown.is_set():
        job_id = webserver.job_counter
        def task():
            result = webserver.data_ingestor.states_mean(data['question'])
            return result

        webserver.tasks_runner.add_job(job_id, task)
        webserver.job_counter += 1
        with webserver.tasks_runner.remaining_jobs_lock:
            webserver.tasks_runner.remaining_jobs += 1

        webserver.logger.info("Job %s added to the queue for processing.", job_id)
        return jsonify({"job_id": job_id})

    webserver.logger.error("Server is shutting down, cannot process request.")
    return jsonify({
        "status": "error",
        "reason": "shutting down"
    })
```

```python
# task_runner.py
def add_job(self, job_id, task):
    job_info = {
        'job_id': job_id,
        'status': 'running',
        'task' : task
    }

    self.jobs[job_id] = job_info
    self.queue.put(job_info)

def run(self):
    while not self.threadpool.graceful_shutdown.is_set():
        try:
            job_info = self.threadpool.queue.get(timeout=1.0)
            job_id = job_info['job_id']
            task = job_info['task']
            result = task()

            with open(f'results/{job_id}', 'w', encoding='utf-8') as f:
                json.dump(result, f)

            job_info['status'] = 'done'
            self.threadpool.queue.task_done()

            with self.threadpool.remaining_jobs_lock:
                self.threadpool.remaining_jobs -= 1

        except Empty:
            continue
```

---

## Procesare statistică a datelor

Metode implementate în clasa `DataIngestor`:

- `states_mean`: media valorilor pentru fiecare stat  
- `state_mean`: media valorilor pentru un singur stat  
- `global_mean`: media globală  
- `diff_from_mean`: diferența față de media globală pentru fiecare stat  
- `state_diff_from_mean`: diferența față de media globală pentru un stat  
- `mean_by_category`: media pe segmente (`Stratification1`) din categorii (`StratificationCategory1`) pentru fiecare stat  
- `state_mean_by_category`: același lucru, dar pentru un singur stat  

---

## Analiză personală

Consider că tema este foarte utilă deoarece combină:
- programarea concurentă (thread-uri, sincronizare, resurse partajate)
- dezvoltarea de API-uri web cu Flask
- analiza datelor

### Posibile îmbunătățiri:
- caching pentru rezultate frecvente  
- paralelizare internă a procesării  
- utilizarea unei baze de date pentru stocare (ex: SQLite, PostgreSQL)

---

## Implementare

Funcționalități implementate:
- Toate endpoint-urile cerute
- Thread pool și job tracking
- Toate calculele statistice
- Logging complet
- Unittesting

### Dificultăți întâmpinate:
1. Mecanismul de shutdown controlat
2. Parsarea corectă a CSV-urilor

### Lucruri interesante:
- `Event` pentru shutdown-ul workerilor  
- `Queue` vs. alte structuri thread-safe  
- Closure-uri pentru capturarea contextului jobului  

---

## Resurse utilizate

- [Laborator 2 ASC](https://ocw.cs.pub.ro/courses/asc/laboratoare/02)  
- [Laborator 3 ASC](https://ocw.cs.pub.ro/courses/asc/laboratoare/03)  
- [Documentația Python - Logging](https://docs.python.org/3/library/logging.html)  
- [Documentația Python - Unittest](https://docs.python.org/3/library/unittest.html)  
- [Flask Mega-Tutorial](https://blog.miguelgrinberg.com/post/the-flask-mega-tutorial-part-i-hello-world)

---

## Git

[Link către repository](https://github.com/VicentiuDuta/ASC-Tema1-LeStatsSportif)