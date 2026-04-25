"""Production-Grade School EI Dashboard

Features:
- Secure login (hashed passwords, DB users)
- SQLite database (users + marks)
- Admin: upload real data (CSV)
- Role-based access (Admin / Teacher / Parent)
- Drill-down: School → Class → Student
- Subject analytics + pass/fail heatmap

Run:
  streamlit run school_reporting_system_streamlit.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import sqlite3
import hashlib
import datetime as dt
from typing import Optional, Tuple

# ---------------- DB ----------------
conn = sqlite3.connect("school.db", check_same_thread=False)
conn.execute("PRAGMA foreign_keys = ON;")

CURRENT_ACADEMIC_YEAR = "2025-2026"


def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def _table_has_column(table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    return column in cols


def _table_exists(table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _foreign_key_refs(table: str) -> list[tuple[str, str]]:
    """
    Returns list of (from_column, ref_table) for a given table.
    """
    cur = conn.cursor()
    cur.execute(f"PRAGMA foreign_key_list({table})")
    out: list[tuple[str, str]] = []
    for row in cur.fetchall():
        # row schema: (id, seq, table, from, to, on_update, on_delete, match)
        out.append((str(row[3]), str(row[2])))
    return out


def _ensure_schema(cur: sqlite3.Cursor) -> None:
    # Users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT,
            role TEXT
        )
    """)

    # Master tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS school_master (
            school_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_name TEXT NOT NULL UNIQUE
        )
    """)

    # Student master (personal details, independent of class assignment)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS student_master (
            student_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_name TEXT NOT NULL,
            father_name TEXT,
            mother_name TEXT,
            father_contact TEXT,
            mother_contact TEXT,
            address TEXT,
            UNIQUE (student_name, father_contact, mother_contact)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS class_master (
            class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id INTEGER NOT NULL,
            class TEXT NOT NULL,
            section TEXT NOT NULL,
            Academic_Year TEXT NOT NULL DEFAULT '2025-2026',
            class_teacher INTEGER,
            UNIQUE (school_id, class, section),
            FOREIGN KEY (school_id) REFERENCES school_master(school_id) ON DELETE CASCADE,
            FOREIGN KEY (class_teacher) REFERENCES teacher_master(teacher_id) ON DELETE SET NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS subject_master (
            subject_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id INTEGER NOT NULL,
            subject TEXT NOT NULL,
            UNIQUE (school_id, subject),
            FOREIGN KEY (school_id) REFERENCES school_master(school_id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS student_class (
            student_class_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER NOT NULL,
            class_id INTEGER NOT NULL,
            student TEXT NOT NULL,
            roll_no TEXT,
            academic_year TEXT NOT NULL DEFAULT '2025-2026',
            UNIQUE (student_id, class_id, academic_year),
            FOREIGN KEY (class_id) REFERENCES class_master(class_id) ON DELETE CASCADE,
            FOREIGN KEY (student_id) REFERENCES student_master(student_id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS exam_master (
            exam_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id INTEGER NOT NULL,
            exam TEXT NOT NULL,
            academic_year TEXT NOT NULL DEFAULT '2025-2026',
            start_date TEXT,
            end_date TEXT,
            UNIQUE (school_id, exam),
            FOREIGN KEY (school_id) REFERENCES school_master(school_id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS teacher_master (
            teacher_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id INTEGER NOT NULL,
            teacher_name TEXT NOT NULL,
            UNIQUE (school_id, teacher_name),
            FOREIGN KEY (school_id) REFERENCES school_master(school_id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS teacher_class_sub (
            teacher_class_sub_id INTEGER PRIMARY KEY AUTOINCREMENT,
            teacher_id INTEGER NOT NULL,
            class_id INTEGER NOT NULL,
            subject_id INTEGER NOT NULL,
            UNIQUE (teacher_id, class_id, subject_id),
            FOREIGN KEY (teacher_id) REFERENCES teacher_master(teacher_id) ON DELETE CASCADE,
            FOREIGN KEY (class_id) REFERENCES class_master(class_id) ON DELETE CASCADE,
            FOREIGN KEY (subject_id) REFERENCES subject_master(subject_id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS marks (
            marks_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_class_id INTEGER NOT NULL,
            subject_id INTEGER NOT NULL,
            exam_id INTEGER NOT NULL,
            marks INTEGER NOT NULL,
            UNIQUE (student_class_id, subject_id, exam_id),
            FOREIGN KEY (student_class_id) REFERENCES student_class(student_class_id) ON DELETE CASCADE,
            FOREIGN KEY (subject_id) REFERENCES subject_master(subject_id) ON DELETE CASCADE,
            FOREIGN KEY (exam_id) REFERENCES exam_master(exam_id) ON DELETE CASCADE
        )
    """)


def _migrate_student_class_pk_and_marks(cur: sqlite3.Cursor) -> None:
    """
    Migrate older DBs to:
    - student_class(student_class_id PK, student_id FK, class_id, ...)
    - marks(student_class_id FK, exam_id, subject_id, marks)
    """
    if _table_exists("student_class") and not _table_has_column("student_class", "student_class_id"):
        cur.execute("ALTER TABLE student_class RENAME TO student_class_old_no_pk")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS student_class (
                student_class_id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id INTEGER NOT NULL,
                class_id INTEGER NOT NULL,
                student TEXT NOT NULL,
                roll_no TEXT,
                academic_year TEXT NOT NULL DEFAULT '2025-2026',
                UNIQUE (student_id, class_id, academic_year),
                FOREIGN KEY (class_id) REFERENCES class_master(class_id) ON DELETE CASCADE,
                FOREIGN KEY (student_id) REFERENCES student_master(student_id) ON DELETE CASCADE
            )
        """)
        # Preserve existing mappings; academic_year may be missing in older DBs.
        if _table_has_column("student_class_old_no_pk", "academic_year"):
            cur.execute("""
                INSERT INTO student_class (student_id, class_id, student, roll_no, academic_year)
                SELECT student_id, class_id, student, roll_no, COALESCE(academic_year, '2025-2026')
                FROM student_class_old_no_pk
            """)
        else:
            cur.execute("""
                INSERT INTO student_class (student_id, class_id, student, roll_no, academic_year)
                SELECT student_id, class_id, student, roll_no, '2025-2026'
                FROM student_class_old_no_pk
            """)

    # Migrate marks if it still stores student_id.
    if _table_exists("marks") and _table_has_column("marks", "student_id") and not _table_has_column("marks", "student_class_id"):
        cur.execute("ALTER TABLE marks RENAME TO marks_old_student_id")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS marks (
                marks_id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_class_id INTEGER NOT NULL,
                subject_id INTEGER NOT NULL,
                exam_id INTEGER NOT NULL,
                marks INTEGER NOT NULL,
                UNIQUE (student_class_id, subject_id, exam_id),
                FOREIGN KEY (student_class_id) REFERENCES student_class(student_class_id) ON DELETE CASCADE,
                FOREIGN KEY (subject_id) REFERENCES subject_master(subject_id) ON DELETE CASCADE,
                FOREIGN KEY (exam_id) REFERENCES exam_master(exam_id) ON DELETE CASCADE
            )
        """)
        # Old schemas had one student_class row per student_id, so this join is stable.
        cur.execute("""
            INSERT INTO marks (marks_id, student_class_id, subject_id, exam_id, marks)
            SELECT m.marks_id, sc.student_class_id, m.subject_id, m.exam_id, m.marks
            FROM marks_old_student_id m
            JOIN student_class sc ON sc.student_id = m.student_id
        """)


def _migrate_schema_additions(cur: sqlite3.Cursor) -> None:
    """
    Backfill schema changes for existing DBs (SQLite has limited ALTER capabilities).
    - Adds academic year columns to student/exam
    - Adds Academic_Year + class_teacher to class_master
    - Ensures teacher tables exist
    """
    # Ensure new tables exist first (class_master FK references teacher_master in new DBs)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teacher_master (
            teacher_id INTEGER PRIMARY KEY AUTOINCREMENT,
            school_id INTEGER NOT NULL,
            teacher_name TEXT NOT NULL,
            UNIQUE (school_id, teacher_name),
            FOREIGN KEY (school_id) REFERENCES school_master(school_id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teacher_class_sub (
            teacher_class_sub_id INTEGER PRIMARY KEY AUTOINCREMENT,
            teacher_id INTEGER NOT NULL,
            class_id INTEGER NOT NULL,
            subject_id INTEGER NOT NULL,
            UNIQUE (teacher_id, class_id, subject_id),
            FOREIGN KEY (teacher_id) REFERENCES teacher_master(teacher_id) ON DELETE CASCADE,
            FOREIGN KEY (class_id) REFERENCES class_master(class_id) ON DELETE CASCADE,
            FOREIGN KEY (subject_id) REFERENCES subject_master(subject_id) ON DELETE CASCADE
        )
    """)

    # Table rename: student_master -> student_class (keep existing DBs working)
    if _table_exists("student_master") and not _table_exists("student_class"):
        cur.execute("ALTER TABLE student_master RENAME TO student_class")

    # Add additive columns for older DBs (SQLite ALTER TABLE is limited but supports ADD COLUMN)
    if _table_exists("exam_master"):
        if not _table_has_column("exam_master", "academic_year"):
            cur.execute("ALTER TABLE exam_master ADD COLUMN academic_year TEXT NOT NULL DEFAULT '2025-2026'")
        if not _table_has_column("exam_master", "start_date"):
            cur.execute("ALTER TABLE exam_master ADD COLUMN start_date TEXT")
        if not _table_has_column("exam_master", "end_date"):
            cur.execute("ALTER TABLE exam_master ADD COLUMN end_date TEXT")

    if _table_exists("student_class"):
        if not _table_has_column("student_class", "academic_year"):
            cur.execute("ALTER TABLE student_class ADD COLUMN academic_year TEXT NOT NULL DEFAULT '2025-2026'")

    if _table_exists("class_master"):
        if not _table_has_column("class_master", "Academic_Year"):
            cur.execute("ALTER TABLE class_master ADD COLUMN Academic_Year TEXT NOT NULL DEFAULT '2025-2026'")
        if not _table_has_column("class_master", "class_teacher"):
            cur.execute("ALTER TABLE class_master ADD COLUMN class_teacher INTEGER")

def _migrate_student_master_and_links(cur: sqlite3.Cursor) -> None:
    """
    Adds `student_master` (requested) and ensures student_id is linked everywhere.

    For existing DBs that already have `student_class(student_id, student, ...)`:
    - Creates `student_master` if missing.
    - Inserts one row per `student_class.student_id` into `student_master` (preserving IDs).
    - Rebuilds `marks` to reference `student_master` (SQLite requires table rebuild for FK changes).
    """
    # Ensure student_master exists (new schema has it, but old DBs may not)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS student_master (
            student_id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_name TEXT NOT NULL,
            father_name TEXT,
            mother_name TEXT,
            father_contact TEXT,
            mother_contact TEXT,
            address TEXT,
            UNIQUE (student_name, father_contact, mother_contact)
        )
    """)

    # Backfill student_master from existing student_class rows (preserve student_id)
    if _table_exists("student_class") and _table_has_column("student_class", "student_id") and _table_has_column("student_class", "student"):
        cur.execute("""
            INSERT OR IGNORE INTO student_master (student_id, student_name)
            SELECT student_id, student
            FROM student_class
            WHERE student_id IS NOT NULL AND TRIM(COALESCE(student, '')) <> ''
        """)


def _get_or_create_school(cur: sqlite3.Cursor, school_name: str) -> int:
    school_name = str(school_name).strip()
    cur.execute("INSERT OR IGNORE INTO school_master (school_name) VALUES (?)", (school_name,))
    cur.execute("SELECT school_id FROM school_master WHERE school_name=?", (school_name,))
    return int(cur.fetchone()[0])


def _get_or_create_class(
    cur: sqlite3.Cursor,
    school_id: int,
    cls: str,
    section: str,
    academic_year: Optional[str] = None,
    class_teacher_id: Optional[int] = None,
) -> int:
    cls = str(cls).strip()
    section = str(section).strip()
    academic_year = CURRENT_ACADEMIC_YEAR if _normalize_str(academic_year or "") == "" else str(academic_year).strip()
    cur.execute(
        """
        INSERT OR IGNORE INTO class_master (school_id, class, section, Academic_Year, class_teacher)
        VALUES (?, ?, ?, ?, ?)
        """,
        (school_id, cls, section, academic_year, class_teacher_id),
    )
    # If the class-section already exists, treat "Add / Save" as an update for year/teacher.
    cur.execute(
        """
        UPDATE class_master
        SET Academic_Year = ?, class_teacher = ?
        WHERE school_id = ? AND class = ? AND section = ?
        """,
        (academic_year, class_teacher_id, school_id, cls, section),
    )
    cur.execute(
        "SELECT class_id FROM class_master WHERE school_id=? AND class=? AND section=?",
        (school_id, cls, section),
    )
    return int(cur.fetchone()[0])


def _get_or_create_subject(cur: sqlite3.Cursor, school_id: int, subject: str) -> int:
    subject = str(subject).strip()
    cur.execute(
        "INSERT OR IGNORE INTO subject_master (school_id, subject) VALUES (?, ?)",
        (school_id, subject),
    )
    cur.execute(
        "SELECT subject_id FROM subject_master WHERE school_id=? AND subject=?",
        (school_id, subject),
    )
    return int(cur.fetchone()[0])


def _get_or_create_exam(cur: sqlite3.Cursor, school_id: int, exam: str, academic_year: Optional[str] = None) -> int:
    exam = str(exam).strip()
    academic_year = CURRENT_ACADEMIC_YEAR if _normalize_str(academic_year or "") == "" else str(academic_year).strip()
    # Backwards-compatible insert for older DBs (but we migrate first on startup)
    if _table_has_column("exam_master", "academic_year") and _table_has_column("exam_master", "start_date") and _table_has_column("exam_master", "end_date"):
        cur.execute(
            "INSERT OR IGNORE INTO exam_master (school_id, exam, academic_year, start_date, end_date) VALUES (?, ?, ?, NULL, NULL)",
            (school_id, exam, academic_year),
        )
    elif _table_has_column("exam_master", "academic_year"):
        cur.execute(
            "INSERT OR IGNORE INTO exam_master (school_id, exam, academic_year) VALUES (?, ?, ?)",
            (school_id, exam, academic_year),
        )
    else:
        cur.execute(
            "INSERT OR IGNORE INTO exam_master (school_id, exam) VALUES (?, ?)",
            (school_id, exam),
        )
    cur.execute("SELECT exam_id FROM exam_master WHERE school_id=? AND exam=?", (school_id, exam))
    return int(cur.fetchone()[0])


def _get_or_create_teacher(cur: sqlite3.Cursor, school_id: int, teacher_name: str) -> int:
    teacher_name = str(teacher_name).strip()
    cur.execute(
        "INSERT OR IGNORE INTO teacher_master (school_id, teacher_name) VALUES (?, ?)",
        (school_id, teacher_name),
    )
    cur.execute(
        "SELECT teacher_id FROM teacher_master WHERE school_id=? AND teacher_name=?",
        (school_id, teacher_name),
    )
    return int(cur.fetchone()[0])


def _get_or_create_teacher_class_sub(cur: sqlite3.Cursor, teacher_id: int, class_id: int, subject_id: int) -> int:
    cur.execute(
        """
        INSERT OR IGNORE INTO teacher_class_sub (teacher_id, class_id, subject_id)
        VALUES (?, ?, ?)
        """,
        (teacher_id, class_id, subject_id),
    )
    cur.execute(
        """
        SELECT teacher_class_sub_id
        FROM teacher_class_sub
        WHERE teacher_id=? AND class_id=? AND subject_id=?
        """,
        (teacher_id, class_id, subject_id),
    )
    return int(cur.fetchone()[0])


def _get_or_create_student(cur: sqlite3.Cursor, class_id: int, student: str, roll_no: Optional[str] = None) -> int:
    student = str(student).strip()
    roll_no = None if roll_no is None or (isinstance(roll_no, float) and np.isnan(roll_no)) else str(roll_no).strip()

    # If already mapped for this class, ensure a student_master row exists and return id.
    cur.execute(
        """
        SELECT student_id, roll_no
        FROM student_class
        WHERE class_id=? AND student=? AND COALESCE(academic_year, ?) = ?
        """,
        (class_id, student, CURRENT_ACADEMIC_YEAR, CURRENT_ACADEMIC_YEAR),
    )
    existing = cur.fetchone()
    if existing:
        student_id = int(existing[0])
        cur.execute("INSERT OR IGNORE INTO student_master (student_id, student_name) VALUES (?, ?)", (student_id, student))
    else:
        # Create student_master first to generate a stable student_id, then map into student_class.
        cur.execute("INSERT INTO student_master (student_name) VALUES (?)", (student,))
        student_id = int(cur.lastrowid)
        if _table_has_column("student_class", "academic_year"):
            cur.execute(
                "INSERT OR IGNORE INTO student_class (student_id, class_id, student, roll_no, academic_year) VALUES (?, ?, ?, ?, ?)",
                (student_id, class_id, student, roll_no, CURRENT_ACADEMIC_YEAR),
            )
        else:
            cur.execute(
                "INSERT OR IGNORE INTO student_class (student_id, class_id, student, roll_no) VALUES (?, ?, ?, ?)",
                (student_id, class_id, student, roll_no),
            )

    if roll_no:
        cur.execute(
            """
            UPDATE student_class
            SET roll_no = COALESCE(roll_no, ?)
            WHERE class_id=? AND student=? AND COALESCE(academic_year, ?) = ?
            """,
            (roll_no, class_id, student, CURRENT_ACADEMIC_YEAR, CURRENT_ACADEMIC_YEAR),
        )
    if _table_has_column("student_class", "academic_year"):
        cur.execute(
            """
            UPDATE student_class
            SET academic_year = COALESCE(academic_year, ?)
            WHERE class_id=? AND student=? AND COALESCE(academic_year, ?) = ?
            """,
            (CURRENT_ACADEMIC_YEAR, class_id, student, CURRENT_ACADEMIC_YEAR, CURRENT_ACADEMIC_YEAR),
        )

    return int(student_id)


def _get_student_class_id(cur: sqlite3.Cursor, class_id: int, student_id: int, academic_year: Optional[str] = None) -> int:
    academic_year = CURRENT_ACADEMIC_YEAR if _normalize_str(academic_year or "") == "" else str(academic_year).strip()
    cur.execute(
        """
        SELECT student_class_id
        FROM student_class
        WHERE class_id=? AND student_id=? AND COALESCE(academic_year, ?) = ?
        """,
        (class_id, student_id, academic_year, academic_year),
    )
    row = cur.fetchone()
    if row:
        return int(row[0])

    # Backfill mapping if missing (should be rare for migrated DBs)
    cur.execute("SELECT student_name FROM student_master WHERE student_id=?", (student_id,))
    srow = cur.fetchone()
    student_name = _normalize_str(srow[0]) if srow else ""
    cur.execute(
        """
        INSERT INTO student_class (student_id, class_id, student, roll_no, academic_year)
        VALUES (?, ?, ?, NULL, ?)
        """,
        (student_id, class_id, student_name or str(student_id), academic_year),
    )
    return int(cur.lastrowid)


def _migrate_legacy_marks(cur: sqlite3.Cursor) -> None:
    """
    If there's an old text-based marks table (class/section/student/subject/exam/marks),
    migrate it into the normalized schema and keep the old data in `marks_legacy`.
    """
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='marks'")
    if not cur.fetchone():
        return

    is_normalized = _table_has_column("marks", "student_class_id")
    if is_normalized:
        return

    # Rename legacy marks table
    cur.execute("ALTER TABLE marks RENAME TO marks_legacy")

    # Create new schema including new marks table
    _ensure_schema(cur)

    # Default school (legacy data had no school dimension)
    default_school_id = _get_or_create_school(cur, "Default School")

    legacy_df = pd.read_sql("SELECT * FROM marks_legacy", conn)
    if legacy_df.empty:
        return

    required_cols = {"class", "section", "student", "subject", "exam", "marks"}
    if not required_cols.issubset(set(legacy_df.columns)):
        return

    legacy_df = legacy_df.copy()
    for c in ["class", "section", "student", "subject", "exam"]:
        legacy_df[c] = legacy_df[c].astype(str).str.strip()
    legacy_df["marks"] = pd.to_numeric(legacy_df["marks"], errors="coerce").fillna(0).astype(int)

    # Build mapping caches to reduce SQL chatter
    class_cache: dict[Tuple[int, str, str], int] = {}
    subject_cache: dict[Tuple[int, str], int] = {}
    exam_cache: dict[Tuple[int, str], int] = {}
    student_cache: dict[Tuple[int, str], int] = {}
    student_class_cache: dict[Tuple[int, int], int] = {}

    for row in legacy_df.itertuples(index=False):
        cls = getattr(row, "class")
        section = getattr(row, "section")
        student = getattr(row, "student")
        subject = getattr(row, "subject")
        exam = getattr(row, "exam")
        marks_val = int(getattr(row, "marks"))

        class_key = (default_school_id, cls, section)
        class_id = class_cache.get(class_key)
        if class_id is None:
            class_id = _get_or_create_class(cur, default_school_id, cls, section)
            class_cache[class_key] = class_id

        student_key = (class_id, student)
        student_id = student_cache.get(student_key)
        if student_id is None:
            student_id = _get_or_create_student(cur, class_id, student)
            student_cache[student_key] = student_id
        sc_key = (class_id, student_id)
        student_class_id = student_class_cache.get(sc_key)
        if student_class_id is None:
            student_class_id = _get_student_class_id(cur, class_id, student_id, academic_year=CURRENT_ACADEMIC_YEAR)
            student_class_cache[sc_key] = student_class_id

        subject_key = (default_school_id, subject)
        subject_id = subject_cache.get(subject_key)
        if subject_id is None:
            subject_id = _get_or_create_subject(cur, default_school_id, subject)
            subject_cache[subject_key] = subject_id

        exam_key = (default_school_id, exam)
        exam_id = exam_cache.get(exam_key)
        if exam_id is None:
            exam_id = _get_or_create_exam(cur, default_school_id, exam)
            exam_cache[exam_key] = exam_id

        cur.execute(
            """
            INSERT OR REPLACE INTO marks (student_class_id, subject_id, exam_id, marks)
            VALUES (?, ?, ?, ?)
            """,
            (student_class_id, subject_id, exam_id, marks_val),
        )


def init_db():
    cur = conn.cursor()

    # Ensure new schema exists (no-op if already created)
    _ensure_schema(cur)

    # If the db was created before this change, migrate legacy marks.
    _migrate_legacy_marks(cur)

    # Backfill additive schema changes (columns/tables) for existing DBs.
    _migrate_schema_additions(cur)

    # Add requested student_master + re-link student_id FKs.
    _migrate_student_master_and_links(cur)

    # Introduce student_class_id PK and re-key marks.
    _migrate_student_class_pk_and_marks(cur)

    # Insert default users if not exists
    users = [
        ("admin", hash_pw("admin123"), "Admin"),
        ("teacher", hash_pw("teacher123"), "Teacher"),
        ("parent", hash_pw("parent123"), "Parent"),
    ]
    for u in users:
        cur.execute("INSERT OR IGNORE INTO users VALUES (?, ?, ?)", u)

    # Ensure at least one school exists so admin screens have defaults
    _get_or_create_school(cur, "Default School")

    conn.commit()


init_db()

# ---------------- HELPER CALCULATIONS ----------------
def get_class_avg(df_exam):
    out = df_exam.groupby("class")["marks"].mean().reset_index()
    out["marks"] = out["marks"].round(0)
    return out

def get_class_section_avg(df_exam):
    out = df_exam.groupby(["class", "section"])["marks"].mean().reset_index()
    out["marks"] = out["marks"].round(0)
    return out


def get_subject_avg(df_exam):
    out = df_exam.groupby("subject")["marks"].mean().reset_index()
    out["marks"] = out["marks"].round(0)
    return out


def get_attendance(df_exam):
    """
    Placeholder attendance generator.
    Returns class-section rows (e.g., 8A, 8B) so School view can chart section-wise.
    """
    out = (
        df_exam[["class", "section"]]
        .dropna(subset=["class", "section"])
        .drop_duplicates()
        .copy()
    )
    out["class"] = out["class"].astype(str).str.strip()
    out["section"] = out["section"].astype(str).str.strip()
    out["class_section"] = out["class"] + out["section"]

    # Sort like 8A, 8B, 9A... even if class is stored as text
    out["_class_num"] = pd.to_numeric(out["class"], errors="coerce")
    out = out.sort_values(
        by=["_class_num", "class", "section"],
        ascending=[True, True, True],
        kind="stable",
    ).drop(columns=["_class_num"])

    out["attendance"] = np.random.randint(75, 100, len(out))
    return out[["class", "section", "class_section", "attendance"]]

# ---------------- LOGIN ----------------
if "user" not in st.session_state:
    st.session_state.user = None
    st.session_state.role = None


def login():
    st.title("🔐 Secure Login")
    u = st.text_input("Username")
    p = st.text_input("Password", type="password")

    if st.button("Login"):
        cur = conn.cursor()
        cur.execute("SELECT password, role FROM users WHERE username=?", (u,))
        row = cur.fetchone()

        if row and row[0] == hash_pw(p):
            st.session_state.user = u
            st.session_state.role = row[1]
            st.success("Login successful")
            st.rerun()
        else:
            st.error("Invalid credentials")


def logout():
    st.session_state.user = None
    st.session_state.role = None
    st.rerun()


if not st.session_state.user:
    login()
    st.stop()

st.sidebar.success(f"Logged in: {st.session_state.user} ({st.session_state.role})")
st.sidebar.button("Logout", on_click=logout)

# ---------------- DATA LOAD ----------------
def load_data():
    # Joined "reporting" dataframe to keep the rest of the app working
    return pd.read_sql(
        """
        SELECT
            sm.school_name AS school,
            cm.class AS class,
            cm.section AS section,
            COALESCE(smm.student_name, stm.student) AS student,
            subm.subject AS subject,
            em.exam AS exam,
            m.marks AS marks
        FROM marks m
        JOIN student_class stm ON stm.student_class_id = m.student_class_id
        JOIN student_master smm ON smm.student_id = stm.student_id
        JOIN class_master cm ON cm.class_id = stm.class_id
        JOIN school_master sm ON sm.school_id = cm.school_id
        JOIN subject_master subm ON subm.subject_id = m.subject_id
        JOIN exam_master em ON em.exam_id = m.exam_id
        """,
        conn,
    )


df = load_data()

def _normalize_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _import_students_csv(csv_df: pd.DataFrame) -> Tuple[int, int]:
    """
    Student Class bulk upload.

    Expected columns:
      class_id, student_id
    Optional:
      roll_no, Academic_Year (or academic_year)
    """
    df_in = csv_df.copy()
    df_in.columns = [str(c).strip() for c in df_in.columns]

    required = ["class_id", "student_id"]
    missing = [c for c in required if c not in df_in.columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")

    academic_year_col = "Academic_Year" if "Academic_Year" in df_in.columns else ("academic_year" if "academic_year" in df_in.columns else None)

    df_in["class_id"] = pd.to_numeric(df_in["class_id"], errors="coerce")
    df_in["student_id"] = pd.to_numeric(df_in["student_id"], errors="coerce")
    df_in = df_in.dropna(subset=["class_id", "student_id"]).copy()
    df_in["class_id"] = df_in["class_id"].astype(int)
    df_in["student_id"] = df_in["student_id"].astype(int)

    if "roll_no" in df_in.columns:
        df_in["roll_no"] = df_in["roll_no"].apply(lambda v: None if _normalize_str(v) == "" else _normalize_str(v))
    else:
        df_in["roll_no"] = None

    if academic_year_col:
        df_in["academic_year"] = df_in[academic_year_col].apply(
            lambda v: CURRENT_ACADEMIC_YEAR if _normalize_str(v) == "" else _normalize_str(v)
        )
    else:
        df_in["academic_year"] = CURRENT_ACADEMIC_YEAR

    inserted = 0
    updated = 0

    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        for row in df_in.itertuples(index=False):
            class_id = int(getattr(row, "class_id"))
            student_id = int(getattr(row, "student_id"))
            roll_no = getattr(row, "roll_no")
            academic_year = getattr(row, "academic_year")

            # Validate foreign keys and fetch the student display name.
            cur.execute("SELECT 1 FROM class_master WHERE class_id=?", (class_id,))
            if not cur.fetchone():
                raise ValueError(f"class_id not found in class_master: {class_id}")

            cur.execute("SELECT student_name FROM student_master WHERE student_id=?", (student_id,))
            srow = cur.fetchone()
            if not srow:
                raise ValueError(f"student_id not found in student_master: {student_id}")
            student_name = _normalize_str(srow[0])
            if student_name == "":
                raise ValueError(f"student_name is blank for student_id: {student_id}")

            cur.execute(
                """
                SELECT student_class_id
                FROM student_class
                WHERE student_id=? AND class_id=? AND COALESCE(academic_year, ?) = ?
                """,
                (student_id, class_id, academic_year, academic_year),
            )
            existing = cur.fetchone()
            if existing:
                cur.execute(
                    """
                    UPDATE student_class
                    SET student=?,
                        roll_no=?,
                        academic_year=?
                    WHERE student_class_id=?
                    """,
                    (student_name, roll_no, academic_year, int(existing[0])),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO student_class (student_id, class_id, student, roll_no, academic_year)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (student_id, class_id, student_name, roll_no, academic_year),
                )
                inserted += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return inserted, updated


def _import_marks_csv(csv_df: pd.DataFrame) -> Tuple[int, int]:
    """
    Expected columns:
      student_class_id, subject_id, exam_id, marks
    """

    required = ["student_class_id", "subject_id", "exam_id", "marks"]
    missing = [c for c in required if c not in csv_df.columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")

    df_in = csv_df.copy()

    # Convert types
    df_in["student_class_id"] = pd.to_numeric(df_in["student_class_id"], errors="coerce")
    df_in["subject_id"] = pd.to_numeric(df_in["subject_id"], errors="coerce")
    df_in["exam_id"] = pd.to_numeric(df_in["exam_id"], errors="coerce")
    df_in["marks"] = pd.to_numeric(df_in["marks"], errors="coerce")

    df_in = df_in.dropna(subset=["student_class_id", "subject_id", "exam_id", "marks"]).copy()

    df_in["student_class_id"] = df_in["student_class_id"].astype(int)
    df_in["subject_id"] = df_in["subject_id"].astype(int)
    df_in["exam_id"] = df_in["exam_id"].astype(int)
    df_in["marks"] = df_in["marks"].astype(int)

    inserted = 0
    updated = 0

    cur = conn.cursor()
    cur.execute("BEGIN")

    try:
        for row in df_in.itertuples(index=False):
            sc_id = row.student_class_id
            subject_id = row.subject_id
            exam_id = row.exam_id
            marks_val = row.marks

            # Validate FK existence
            cur.execute("SELECT 1 FROM student_class WHERE student_class_id=?", (sc_id,))
            if not cur.fetchone():
                raise ValueError(f"Invalid student_class_id: {sc_id}")

            cur.execute("SELECT 1 FROM subject_master WHERE subject_id=?", (subject_id,))
            if not cur.fetchone():
                raise ValueError(f"Invalid subject_id: {subject_id}")

            cur.execute("SELECT 1 FROM exam_master WHERE exam_id=?", (exam_id,))
            if not cur.fetchone():
                raise ValueError(f"Invalid exam_id: {exam_id}")

            # Check existing
            cur.execute("""
                SELECT marks_id FROM marks
                WHERE student_class_id=? AND subject_id=? AND exam_id=?
            """, (sc_id, subject_id, exam_id))

            existing = cur.fetchone()

            if existing:
                cur.execute("""
                    UPDATE marks
                    SET marks=?
                    WHERE marks_id=?
                """, (marks_val, existing[0]))
                updated += 1
            else:
                cur.execute("""
                    INSERT INTO marks (student_class_id, subject_id, exam_id, marks)
                    VALUES (?, ?, ?, ?)
                """, (sc_id, subject_id, exam_id, marks_val))
                inserted += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    return inserted, updated


def _import_student_master_csv(csv_df: pd.DataFrame) -> Tuple[int, int]:
    """
    Expected columns:
      student_name

    Optional columns:
      student_id (to update existing records by ID)
      father_name, mother_name, father_contact, mother_contact, address

    Behavior:
    - If `student_id` is present and exists: UPDATE that row.
    - Else: INSERT a new row (or IGNORE if it violates the UNIQUE constraint).
    """
    if "student_name" not in csv_df.columns:
        raise ValueError("Missing columns: student_name")

    df_in = csv_df.copy()

    optional_cols = ["father_name", "mother_name", "father_contact", "mother_contact", "address"]
    for c in ["student_name"] + optional_cols:
        if c in df_in.columns:
            df_in[c] = df_in[c].apply(lambda v: None if _normalize_str(v) == "" else _normalize_str(v))

    has_student_id = "student_id" in df_in.columns
    if has_student_id:
        df_in["student_id"] = pd.to_numeric(df_in["student_id"], errors="coerce")

    df_in = df_in[df_in["student_name"].notna()].copy()

    inserted = 0
    updated = 0

    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        for row in df_in.itertuples(index=False):
            student_name = getattr(row, "student_name")
            father_name = getattr(row, "father_name") if hasattr(row, "father_name") else None
            mother_name = getattr(row, "mother_name") if hasattr(row, "mother_name") else None
            father_contact = getattr(row, "father_contact") if hasattr(row, "father_contact") else None
            mother_contact = getattr(row, "mother_contact") if hasattr(row, "mother_contact") else None
            address = getattr(row, "address") if hasattr(row, "address") else None

            student_id_val = None
            if has_student_id:
                sid = getattr(row, "student_id")
                if sid is not None and not (isinstance(sid, float) and np.isnan(sid)):
                    student_id_val = int(sid)

            if student_id_val is not None:
                cur.execute("SELECT 1 FROM student_master WHERE student_id=?", (student_id_val,))
                if cur.fetchone():
                    cur.execute(
                        """
                        UPDATE student_master
                        SET student_name = COALESCE(?, student_name),
                            father_name = ?,
                            mother_name = ?,
                            father_contact = ?,
                            mother_contact = ?,
                            address = ?
                        WHERE student_id = ?
                        """,
                        (
                            _normalize_str(student_name) or None,
                            father_name,
                            mother_name,
                            father_contact,
                            mother_contact,
                            address,
                            student_id_val,
                        ),
                    )
                    updated += 1
                    continue

                cur.execute(
                    """
                    INSERT OR IGNORE INTO student_master
                        (student_id, student_name, father_name, mother_name, father_contact, mother_contact, address)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        student_id_val,
                        _normalize_str(student_name),
                        father_name,
                        mother_name,
                        father_contact,
                        mother_contact,
                        address,
                    ),
                )
                if cur.rowcount:
                    inserted += 1
                continue

            cur.execute(
                """
                INSERT OR IGNORE INTO student_master
                    (student_name, father_name, mother_name, father_contact, mother_contact, address)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    _normalize_str(student_name),
                    father_name,
                    mother_name,
                    father_contact,
                    mother_contact,
                    address,
                ),
            )
            if cur.rowcount:
                inserted += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return inserted, updated


def _admin_panel():
    st.subheader("Admin Panel")

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs(
        [
            "Schools",
            "Teachers",
            "Class",
            "Subject",
            "Teacher-Class-Subject",
            "Student",
            "Student Class",
            "Exams",
            "Mark",
        ]
    )

    cur = conn.cursor()

    with tab1:
        st.markdown("**School Master**")
        with st.form("add_school"):
            school_name = st.text_input("School Name")
            submitted = st.form_submit_button("Add / Save School")
            if submitted:
                if _normalize_str(school_name) == "":
                    st.error("School name is required.")
                else:
                    _get_or_create_school(cur, school_name)
                    conn.commit()
                    st.success("Saved.")
                    st.rerun()
        st.dataframe(pd.read_sql("SELECT * FROM school_master ORDER BY school_name", conn), use_container_width=True)

    with tab2:
        st.markdown("**Teacher Master**")
        schools = pd.read_sql("SELECT school_id, school_name FROM school_master ORDER BY school_name", conn)
        if schools.empty:
            st.warning("Create a school first.")
        else:
            school_name = st.selectbox("School", schools["school_name"].tolist(), key="t_school")
            school_id = int(schools[schools["school_name"] == school_name]["school_id"].iloc[0])
            with st.form("add_teacher"):
                teacher_name = st.text_input("Teacher Name")
                submitted = st.form_submit_button("Add / Save Teacher")
                if submitted:
                    if _normalize_str(teacher_name) == "":
                        st.error("Teacher name is required.")
                    else:
                        _get_or_create_teacher(cur, school_id, teacher_name)
                        conn.commit()
                        st.success("Saved.")
                        st.rerun()

            st.dataframe(
                pd.read_sql(
                    """
                    SELECT tm.teacher_id, sm.school_name, tm.teacher_name
                    FROM teacher_master tm
                    JOIN school_master sm ON sm.school_id = tm.school_id
                    ORDER BY sm.school_name, tm.teacher_name
                    """,
                    conn,
                ),
                use_container_width=True,
            )

    with tab3:
        st.markdown("**Class Master (Class + Section)**")
        schools = pd.read_sql("SELECT school_id, school_name FROM school_master ORDER BY school_name", conn)
        if schools.empty:
            st.warning("Create a school first.")
        else:
            school_name = st.selectbox("School", schools["school_name"].tolist(), key="cls_school")
            school_id = int(schools[schools["school_name"] == school_name]["school_id"].iloc[0])

            teachers = pd.read_sql(
                "SELECT teacher_id, teacher_name FROM teacher_master WHERE school_id=? ORDER BY teacher_name",
                conn,
                params=(school_id,),
            )
            teacher_options = ["(None)"] + (teachers["teacher_name"].tolist() if not teachers.empty else [])
            with st.form("add_class"):
                cls = st.text_input("Class (e.g., 8, IX, Grade 10)")
                section = st.text_input("Section (e.g., A, B)")
                academic_year = st.text_input("Academic Year", value=CURRENT_ACADEMIC_YEAR)
                class_teacher_name = st.selectbox("Class Teacher", teacher_options)
                submitted = st.form_submit_button("Add / Save Class-Section")
                if submitted:
                    if _normalize_str(cls) == "" or _normalize_str(section) == "":
                        st.error("Class and Section are required.")
                    else:
                        class_teacher_id = None
                        if class_teacher_name != "(None)" and not teachers.empty:
                            class_teacher_id = int(
                                teachers.loc[teachers["teacher_name"] == class_teacher_name, "teacher_id"].iloc[0]
                            )
                        _get_or_create_class(cur, school_id, cls, section, academic_year, class_teacher_id)
                        conn.commit()
                        st.success("Saved.")
                        st.rerun()

            st.dataframe(
                pd.read_sql(
                    """
                    SELECT cm.class_id, sm.school_name, cm.class, cm.section,
                           COALESCE(cm.Academic_Year, ?) AS Academic_Year,
                           COALESCE(tm.teacher_name, '') AS class_teacher
                    FROM class_master cm
                    JOIN school_master sm ON sm.school_id = cm.school_id
                    LEFT JOIN teacher_master tm ON tm.teacher_id = cm.class_teacher
                    ORDER BY sm.school_name, cm.class, cm.section
                    """,
                    conn,
                    params=(CURRENT_ACADEMIC_YEAR,),
                ),
                use_container_width=True,
            )

    with tab4:
        st.markdown("**Subject Master**")
        schools = pd.read_sql("SELECT school_id, school_name FROM school_master ORDER BY school_name", conn)
        if schools.empty:
            st.warning("Create a school first.")
        else:
            school_name = st.selectbox("School", schools["school_name"].tolist(), key="sub_school")
            school_id = int(schools[schools["school_name"] == school_name]["school_id"].iloc[0])
            with st.form("add_subject"):
                subject = st.text_input("Subject (e.g., Math, Science)")
                submitted = st.form_submit_button("Add / Save Subject")
                if submitted:
                    if _normalize_str(subject) == "":
                        st.error("Subject is required.")
                    else:
                        _get_or_create_subject(cur, school_id, subject)
                        conn.commit()
                        st.success("Saved.")
                        st.rerun()

            st.dataframe(
                pd.read_sql(
                    """
                    SELECT subm.subject_id, sm.school_name, subm.subject
                    FROM subject_master subm
                    JOIN school_master sm ON sm.school_id = subm.school_id
                    ORDER BY sm.school_name, subm.subject
                    """,
                    conn,
                ),
                use_container_width=True,
            )

    with tab5:
        st.markdown("**Teacher Class Master (Teacher → Class-Section → Subject)**")
        schools = pd.read_sql("SELECT school_id, school_name FROM school_master ORDER BY school_name", conn)
        if schools.empty:
            st.warning("Create a school first.")
        else:
            school_name = st.selectbox("School", schools["school_name"].tolist(), key="tcs_school")
            school_id = int(schools[schools["school_name"] == school_name]["school_id"].iloc[0])

            teachers = pd.read_sql(
                "SELECT teacher_id, teacher_name FROM teacher_master WHERE school_id=? ORDER BY teacher_name",
                conn,
                params=(school_id,),
            )
            classes = pd.read_sql(
                "SELECT class_id, class, section FROM class_master WHERE school_id=? ORDER BY class, section",
                conn,
                params=(school_id,),
            )
            subjects = pd.read_sql(
                "SELECT subject_id, subject FROM subject_master WHERE school_id=? ORDER BY subject",
                conn,
                params=(school_id,),
            )

            if teachers.empty or classes.empty or subjects.empty:
                st.warning("Create teachers, class-sections, and subjects first.")
            else:
                teacher_name = st.selectbox("Teacher", teachers["teacher_name"].tolist(), key="tcs_teacher")
                teacher_id = int(teachers[teachers["teacher_name"] == teacher_name]["teacher_id"].iloc[0])

                class_section_label = classes.apply(lambda r: f"{r['class']}{r['section']}", axis=1).tolist()
                chosen_class_section = st.selectbox("Class-Section", class_section_label, key="tcs_class")
                class_id = int(classes.iloc[class_section_label.index(chosen_class_section)]["class_id"])

                subject_name = st.selectbox("Subject", subjects["subject"].tolist(), key="tcs_subject")
                subject_id = int(subjects[subjects["subject"] == subject_name]["subject_id"].iloc[0])

                if st.button("Add / Save Teacher-Class-Subject"):
                    _get_or_create_teacher_class_sub(cur, teacher_id, class_id, subject_id)
                    conn.commit()
                    st.success("Saved.")
                    st.rerun()

            st.dataframe(
                pd.read_sql(
                    """
                    SELECT
                        tcs.teacher_class_sub_id,
                        sm.school_name,
                        tm.teacher_name,
                        cm.class,
                        cm.section,
                        subm.subject
                    FROM teacher_class_sub tcs
                    JOIN teacher_master tm ON tm.teacher_id = tcs.teacher_id
                    JOIN class_master cm ON cm.class_id = tcs.class_id
                    JOIN subject_master subm ON subm.subject_id = tcs.subject_id
                    JOIN school_master sm ON sm.school_id = tm.school_id
                    ORDER BY sm.school_name, tm.teacher_name, cm.class, cm.section, subm.subject
                    """,
                    conn,
                ),
                use_container_width=True,
            )

    with tab6:
        st.markdown("**Student Master (Personal Details)**")
        st.markdown("Create / update a student profile. Class assignment happens under **Student Class**.")

        st.markdown("**Bulk upload (CSV)**")
        st.markdown(
            "Upload a CSV with column: `student_name` (optional: `student_id,father_name,mother_name,father_contact,mother_contact,address`)."
        )
        sm_file = st.file_uploader("Upload Student Master CSV", type=["csv"], key="student_master_csv")
        if sm_file:
            try:
                sm_df = pd.read_csv(sm_file)
                st.dataframe(sm_df.head(50), use_container_width=True)
                if st.button("Import Student Master CSV"):
                    ins, upd = _import_student_master_csv(sm_df)
                    st.success(f"Imported. Inserted: {ins}, Updated: {upd}")
                    st.rerun()
            except Exception as e:
                st.error(f"Import failed: {e}")

        students = pd.read_sql(
            "SELECT student_id, student_name FROM student_master ORDER BY student_name, student_id",
            conn,
        )
        selected_student_id: Optional[int] = None

        options = ["+ Create new"] + (
            students.apply(lambda r: f"{r['student_name']} (ID: {int(r['student_id'])})", axis=1).tolist()
            if not students.empty
            else []
        )
        chosen = st.selectbox("Student", options, key="sm_pick")
        if chosen != "+ Create new":
            selected_student_id = int(chosen.split("ID:")[1].replace(")", "").strip())

        existing = None
        if selected_student_id is not None:
            existing = pd.read_sql(
                """
                SELECT student_id, student_name, father_name, mother_name, father_contact, mother_contact, address
                FROM student_master
                WHERE student_id = ?
                """,
                conn,
                params=(selected_student_id,),
            )
            if not existing.empty:
                existing = existing.iloc[0].to_dict()
            else:
                existing = None

        with st.form("student_master_form"):
            student_name = st.text_input("Student Name", value="" if not existing else str(existing.get("student_name") or ""))
            father_name = st.text_input("Father Name", value="" if not existing else str(existing.get("father_name") or ""))
            mother_name = st.text_input("Mother Name", value="" if not existing else str(existing.get("mother_name") or ""))
            father_contact = st.text_input(
                "Father Contact",
                value="" if not existing else str(existing.get("father_contact") or ""),
            )
            mother_contact = st.text_input(
                "Mother Contact",
                value="" if not existing else str(existing.get("mother_contact") or ""),
            )
            address = st.text_area("Address", value="" if not existing else str(existing.get("address") or ""))

            submitted = st.form_submit_button("Add / Save Student Master")
            if submitted:
                if _normalize_str(student_name) == "":
                    st.error("Student name is required.")
                else:
                    if selected_student_id is None:
                        cur.execute(
                            """
                            INSERT OR IGNORE INTO student_master
                                (student_name, father_name, mother_name, father_contact, mother_contact, address)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                student_name.strip(),
                                father_name.strip() or None,
                                mother_name.strip() or None,
                                father_contact.strip() or None,
                                mother_contact.strip() or None,
                                address.strip() or None,
                            ),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE student_master
                            SET student_name = ?,
                                father_name = ?,
                                mother_name = ?,
                                father_contact = ?,
                                mother_contact = ?,
                                address = ?
                            WHERE student_id = ?
                            """,
                            (
                                student_name.strip(),
                                father_name.strip() or None,
                                mother_name.strip() or None,
                                father_contact.strip() or None,
                                mother_contact.strip() or None,
                                address.strip() or None,
                                int(selected_student_id),
                            ),
                        )
                    conn.commit()
                    st.success("Saved.")
                    st.rerun()

        st.dataframe(
            pd.read_sql(
                """
                SELECT student_id, student_name, father_name, mother_name, father_contact, mother_contact, address
                FROM student_master
                ORDER BY student_name, student_id
                """,
                conn,
            ),
            use_container_width=True,
        )

    with tab7:
        st.markdown("**Student Class**")
        st.markdown(f"Academic year in DB defaults to `{CURRENT_ACADEMIC_YEAR}`.")

        st.markdown("**Bulk upload (CSV)**")
        st.markdown("Upload a CSV with columns: `class_id,student_id` (optional `roll_no,Academic_Year`).")
        s_file = st.file_uploader("Upload Students CSV", type=["csv"], key="students_csv")
        if s_file:
            try:
                s_df = pd.read_csv(s_file)
                st.dataframe(s_df.head(50), use_container_width=True)
                if st.button("Import Students CSV"):
                    ins, upd = _import_students_csv(s_df)
                    st.success(f"Imported. Inserted: {ins}, Updated: {upd}")
                    st.rerun()
            except Exception as e:
                st.error(f"Import failed: {e}")

        st.divider()
        classes = pd.read_sql(
            """
            SELECT cm.class_id, sm.school_name, cm.class, cm.section
            FROM class_master cm
            JOIN school_master sm ON sm.school_id = cm.school_id
            ORDER BY sm.school_name, cm.class, cm.section
            """,
            conn,
        )
        if classes.empty:
            st.warning("Create class-sections first.")
        else:
            class_label = classes.apply(lambda r: f"{r['school_name']} | {r['class']}{r['section']}", axis=1).tolist()
            selected = st.selectbox("Class-Section", class_label, key="stu_class")
            class_id = int(classes.iloc[class_label.index(selected)]["class_id"])
            with st.form("add_student"):
                student = st.text_input("Student Name")
                roll_no = st.text_input("Roll No (optional)")
                submitted = st.form_submit_button("Add / Save Student")
                if submitted:
                    if _normalize_str(student) == "":
                        st.error("Student name is required.")
                    else:
                        _get_or_create_student(cur, class_id, student, roll_no=roll_no)
                        conn.commit()
                        st.success("Saved.")
                        st.rerun()

            st.dataframe(
                pd.read_sql(
                    """
                    SELECT stm.student_class_id, stm.student_id, sm.school_name, cm.class, cm.section, stm.student, stm.roll_no,
                           COALESCE(stm.academic_year, ?) AS academic_year
                    FROM student_class stm
                    JOIN class_master cm ON cm.class_id = stm.class_id
                    JOIN school_master sm ON sm.school_id = cm.school_id
                    ORDER BY sm.school_name, cm.class, cm.section, stm.student
                    """,
                    conn,
                    params=(CURRENT_ACADEMIC_YEAR,),
                ),
                use_container_width=True,
            )

    with tab8:
        st.markdown("**Exam Master**")
        st.markdown(f"Academic year in DB defaults to `{CURRENT_ACADEMIC_YEAR}`.")
        schools = pd.read_sql("SELECT school_id, school_name FROM school_master ORDER BY school_name", conn)
        if schools.empty:
            st.warning("Create a school first.")
        else:
            school_name = st.selectbox("School", schools["school_name"].tolist(), key="exam_school")
            school_id = int(schools[schools["school_name"] == school_name]["school_id"].iloc[0])

            # Keep this outside the form so enabling/disabling dates works immediately.
            use_dates = st.checkbox("Set start/end dates", value=False, key="exam_use_dates")

            with st.form("add_exam"):
                exam = st.text_input("Exam (e.g., Midterm, Unit Test 1)")
                academic_year = st.text_input("Academic Year", value=CURRENT_ACADEMIC_YEAR)
                start_date = st.date_input(
                    "Start Date",
                    value=dt.date.today(),
                    disabled=(not st.session_state.get("exam_use_dates", False)),
                )
                end_date = st.date_input(
                    "End Date",
                    value=dt.date.today(),
                    disabled=(not st.session_state.get("exam_use_dates", False)),
                )
                submitted = st.form_submit_button("Add / Save Exam")
                if submitted:
                    if _normalize_str(exam) == "":
                        st.error("Exam is required.")
                    else:
                        exam_id = _get_or_create_exam(cur, school_id, exam, academic_year=academic_year)

                        if _table_has_column("exam_master", "academic_year"):
                            cur.execute(
                                "UPDATE exam_master SET academic_year=? WHERE exam_id=?",
                                (str(academic_year).strip(), exam_id),
                            )

                        if use_dates and _table_has_column("exam_master", "start_date") and _table_has_column("exam_master", "end_date"):
                            cur.execute(
                                """
                                UPDATE exam_master
                                SET start_date = ?,
                                    end_date = ?
                                WHERE exam_id=?
                                """,
                                (start_date.isoformat(), end_date.isoformat(), exam_id),
                            )
                        conn.commit()
                        st.success("Saved.")
                        st.rerun()

            st.dataframe(
                pd.read_sql(
                    """
                    SELECT em.exam_id, sm.school_name, em.exam,
                           COALESCE(em.academic_year, ?) AS academic_year,
                           em.start_date, em.end_date
                    FROM exam_master em
                    JOIN school_master sm ON sm.school_id = em.school_id
                    ORDER BY sm.school_name, em.exam
                    """,
                    conn,
                    params=(CURRENT_ACADEMIC_YEAR,),
                ),
                use_container_width=True,
            )

    with tab9:
        st.markdown("**Marks**")
        st.markdown("Upload a CSV with columns: `student_class_id,subject_id,exam_id,marks`.")

        file = st.file_uploader("Upload Marks CSV", type=["csv"], key="marks_csv")
        if file:
            try:
                csv_df = pd.read_csv(file)
                st.dataframe(csv_df.head(50), use_container_width=True)
                if st.button("Import CSV"):
                    ins, upd = _import_marks_csv(csv_df)
                    st.success(f"Imported. Inserted: {ins}, Updated: {upd}")
                    st.rerun()
            except Exception as e:
                st.error(f"Import failed: {e}")

        st.divider()
        st.markdown("**Manual Entry**")
        schools = pd.read_sql("SELECT school_id, school_name FROM school_master ORDER BY school_name", conn)
        if schools.empty:
            st.warning("Create a school first.")
        else:
            school_name = st.selectbox("School", schools["school_name"].tolist(), key="m_school")
            school_id = int(schools[schools["school_name"] == school_name]["school_id"].iloc[0])

            classes = pd.read_sql(
                "SELECT class_id, class, section FROM class_master WHERE school_id=? ORDER BY class, section",
                conn,
                params=(school_id,),
            )
            subjects = pd.read_sql(
                "SELECT subject_id, subject FROM subject_master WHERE school_id=? ORDER BY subject",
                conn,
                params=(school_id,),
            )
            exams = pd.read_sql(
                "SELECT exam_id, exam FROM exam_master WHERE school_id=? ORDER BY exam",
                conn,
                params=(school_id,),
            )

            if classes.empty or subjects.empty or exams.empty:
                st.warning("Create class-sections, subjects, and exams first.")
            else:
                class_section_label = classes.apply(lambda r: f"{r['class']}{r['section']}", axis=1).tolist()
                chosen_class_section = st.selectbox("Class-Section", class_section_label, key="m_class")
                class_id = int(classes.iloc[class_section_label.index(chosen_class_section)]["class_id"])

                students = pd.read_sql(
                    "SELECT student_class_id, student_id, student FROM student_class WHERE class_id=? ORDER BY student",
                    conn,
                    params=(class_id,),
                )
                if students.empty:
                    st.warning("Create students for this class-section first.")
                else:
                    student_name = st.selectbox("Student", students["student"].tolist(), key="m_student")
                    student_class_id = int(students[students["student"] == student_name]["student_class_id"].iloc[0])

                    subject_name = st.selectbox("Subject", subjects["subject"].tolist(), key="m_subject")
                    subject_id = int(subjects[subjects["subject"] == subject_name]["subject_id"].iloc[0])

                    exam_name = st.selectbox("Exam", exams["exam"].tolist(), key="m_exam")
                    exam_id = int(exams[exams["exam"] == exam_name]["exam_id"].iloc[0])

                    marks_val = st.number_input("Marks", min_value=0, max_value=100, value=0, step=1, key="m_marks")

                    if st.button("Save Marks"):
                        cur.execute(
                            """
                            INSERT OR REPLACE INTO marks (student_class_id, subject_id, exam_id, marks)
                            VALUES (?, ?, ?, ?)
                            """,
                            (student_class_id, subject_id, exam_id, int(marks_val)),
                        )
                        conn.commit()
                        st.success("Saved.")
                        st.rerun()

        st.divider()
        st.dataframe(load_data().sort_values(["school", "class", "section", "student", "exam", "subject"]), use_container_width=True)


# ---------------- ADMIN: DATA MANAGEMENT ----------------
if st.session_state.role == "Admin":
    st.sidebar.subheader("Admin Controls")
    admin_view = st.sidebar.radio("Admin Menu", ["Dashboard", "Admin Panel"], index=0)
else:
    admin_view = "Dashboard"

# ---------------- NAVIGATION ----------------
if "level" not in st.session_state:
    st.session_state.level = "school"
    st.session_state.cls = None
    st.session_state.student = None

# Route admin to admin panel (without disrupting session navigation state)
if st.session_state.role == "Admin" and admin_view == "Admin Panel":
    st.title("🛠️ Administration")
    _admin_panel()
    st.stop()

# ---------------- SCHOOL VIEW (UPDATED - TABS + MOBILE FRIENDLY) ----------------
if st.session_state.level == "school":
    st.title("🏫 School Dashboard")

    if df.empty:
        st.warning("No data available. Admin upload required.")
    else:
        if "school" in df.columns and df["school"].nunique() > 1:
            selected_school = st.selectbox("Select School", sorted(df["school"].unique()))
            df = df[df["school"] == selected_school].copy()

        exams = sorted(df["exam"].unique(), reverse=True)

        for i, exam in enumerate(exams):
            with st.expander(f"📘 Exam: {exam}", expanded=(i == 0)):
                edf = df[df["exam"] == exam]

                # Tabs instead of columns (better for mobile)
                tab1, tab2, tab3, tab4 = st.tabs([
                    "Exam",
                    "Subject-wise Avg",
                    "Class-wise Avg",
                    "Attendance"
                ])

                # ---------------- TAB 1: EXAM ----------------
                with tab1:
                    st.subheader("Exam Details")
                    st.metric(label="Exam Name", value=exam)
                    st.metric(label="Total Records", value=len(edf))

                # ---------------- TAB 2: SUBJECT AVG ----------------
                with tab2:
                    st.subheader("Subject-wise Average Marks")
                    subj_avg = get_subject_avg(edf)
                    fig_sub = px.bar(
                        subj_avg,
                        x="subject",
                        y="marks",
                        text_auto=True
                    )
                    fig_sub.update_traces(textposition="outside", texttemplate="%{y:.0f}%")
                    fig_sub.update_yaxes(tickformat=".0f", ticksuffix="%")
                    st.plotly_chart(fig_sub, use_container_width=True, key=f"{fig_sub}_{exam}_{i}")

                # ---------------- TAB 3: CLASS AVG ----------------
                with tab3:
                    st.subheader("Class-wise Average Marks")
                    class_sec_avg = get_class_section_avg(edf).copy()
                    class_sec_avg["class_section"] = (
                        class_sec_avg["class"].astype(str).str.strip()
                        + class_sec_avg["section"].astype(str).str.strip()
                    )

                    # Sort like 8A, 8B, 9A... even if class is stored as text
                    class_sec_avg["_class_num"] = pd.to_numeric(class_sec_avg["class"], errors="coerce")
                    class_sec_avg = class_sec_avg.sort_values(
                        by=["_class_num", "class", "section"],
                        ascending=[True, True, True],
                        kind="stable",
                    )

                    fig_cls = px.bar(
                        class_sec_avg,
                        x="marks",
                        y="class_section",
                        orientation='h',
                        text_auto=True,
                        color="marks",
                        color_continuous_scale="RdYlGn",
                        range_color=(0, 100),
                    )
                    fig_cls.update_traces(textposition="outside", texttemplate="%{x:.0f}%")
                    fig_cls.update_xaxes(tickformat=".0f", ticksuffix="%")
                    fig_cls.update_yaxes(
                        type="category",
                        categoryorder="array",
                        categoryarray=class_sec_avg["class_section"].tolist(),
                        title_text="Class-Section",
                    )
                    fig_cls.update_layout(coloraxis_colorbar=dict(title="Avg %"))
                    st.plotly_chart(fig_cls, use_container_width=True, key=f"{fig_cls}_{exam}_{i}")

                # ---------------- TAB 4: ATTENDANCE ----------------
                with tab4:
                    st.subheader("Class-section-wise Attendance (%)")
                    att = get_attendance(edf)
                    fig_att = px.bar(
                        att,
                        x="attendance",
                        y="class_section",
                        orientation='h',
                        text_auto=True
                    )
                    fig_att.update_traces(textposition="outside")
                    fig_att.update_yaxes(
                        type="category",
                        categoryorder="array",
                        categoryarray=att["class_section"].tolist(),
                        title_text="Class-Section",
                    )
                    st.plotly_chart(fig_att, use_container_width=True, key=f"{fig_att}_{exam}_{i}")

        if st.button("Drill to Class"):
            st.session_state.level = "class"
            st.rerun()

# ---------------- CLASS VIEW (NEW - TABS + MOBILE FRIENDLY) ----------------
if st.session_state.level == "class":
    st.title("🏫 Class Dashboard")

    if df.empty:
        st.warning("No data available. Admin upload required.")
    else:
        # Select Class
        classes = sorted(df["class"].unique())
        selected_class = st.selectbox("Select Class", classes)

        sections = sorted(df[df["class"] == selected_class]["section"].dropna().unique())
        section_options = ["All Sections"] + sections
        selected_section = st.selectbox("Select Section", section_options, index=0)

        if selected_section == "All Sections":
            cdf = df[df["class"] == selected_class]
        else:
            cdf = df[
                (df["class"] == selected_class) &
                (df["section"] == selected_section)
            ]

        if cdf.empty:
            st.warning("No data available for selected class")
        else:
            exams = sorted(cdf["exam"].unique(), reverse=True)

            for i, exam in enumerate(exams):
                with st.expander(f"📘 Exam: {exam}", expanded=(i == 0)):
                    edf = cdf[cdf["exam"] == exam]

                    # Tabs for mobile-friendly layout
                    tab1, tab2, tab3, tab4 = st.tabs([
                        "Exam",
                        "Subject-wise Avg",
                        "Student-wise Avg",
                        "Attendance"
                    ])

                    # -------- TAB 1: EXAM --------
                    with tab1:
                        st.subheader("Exam Details")
                        st.metric("Class", selected_class)
                        st.metric("Section", selected_section)
                        st.metric("Exam", exam)
                        st.metric("Total Records", len(edf))

                    # -------- TAB 2: SUBJECT AVG --------
                    with tab2:
                        st.subheader("Subject-wise Average Marks")
                        subj_avg = edf.groupby("subject")["marks"].mean().reset_index()
                        subj_avg["marks"] = subj_avg["marks"].round(0)
                        fig_sub = px.bar(
                            subj_avg,
                            x="subject",
                            y="marks",
                            text_auto=True
                        )
                        fig_sub.update_traces(textposition="outside", texttemplate="%{y:.0f}%")
                        fig_sub.update_yaxes(tickformat=".0f", ticksuffix="%")
                        st.plotly_chart(fig_sub, use_container_width=True, key=f"{fig_sub}_{exam}_{i}")

                    # -------- TAB 3: STUDENT AVG --------
                    with tab3:
                        st.subheader("Student-wise Average Marks")
                        stu_avg = edf.groupby("student")["marks"].mean().reset_index()
                        stu_avg["marks"] = stu_avg["marks"].round(0)
                        fig_stu = px.bar(
                            stu_avg,
                            x="marks",
                            y="student",
                            orientation='h',
                            text_auto=True
                        )
                        fig_stu.update_traces(textposition="outside", texttemplate="%{x:.0f}%")
                        fig_stu.update_xaxes(tickformat=".0f", ticksuffix="%")
                        st.plotly_chart(fig_stu, use_container_width=True, key=f"{fig_stu}_{exam}_{i}")

                    # -------- TAB 4: ATTENDANCE --------
                    with tab4:
                        st.subheader("Student-wise Attendance (%)")
                        students = edf["student"].unique()
                        att = pd.DataFrame({
                            "student": students,
                            "attendance": np.random.randint(75, 100, len(students))
                        })
                        fig_att = px.bar(
                            att,
                            x="attendance",
                            y="student",
                            orientation='h',
                            text_auto=True
                        )
                        fig_att.update_traces(textposition="outside")
                        st.plotly_chart(fig_att, use_container_width=True, key=f"{fig_att}_{exam}_{i}")

        # Navigation back
        if st.button("⬅ Back to School"):
            st.session_state.level = "school"
            st.rerun()

        if st.button("Drill to Student"):
            st.session_state.level = "student"
            st.rerun()

# ---------------- STUDENT VIEW (NEW - TABS + MOBILE FRIENDLY) ----------------
if st.session_state.level == "student":
    st.title("🎓 Student Dashboard")

    if df.empty:
        st.warning("No data available. Admin upload required.")
    else:
        # Select Class first (to scope students)
        classes = sorted(df["class"].unique())
        selected_class = st.selectbox("Select Class", classes)

        sections = sorted(
            df[df["class"] == selected_class]["section"].dropna().unique()
        )

        selected_section = st.selectbox("Select Section", sections)

        students = sorted(
        df[
            (df["class"] == selected_class) &
            (df["section"] == selected_section)
        ]["student"].unique()
        )

        selected_student = st.selectbox("Select Student", students)

        stf = df[
            (df["class"] == selected_class) &
            (df["section"] == selected_section) &
            (df["student"] == selected_student)
        ]

        if stf.empty:
            st.warning("No data available for selected student")
        else:
            exams = sorted(stf["exam"].unique(), reverse=True)

            for i, exam in enumerate(exams):
                with st.expander(f"📘 Exam: {exam}", expanded=(i == 0)):
                    edf = stf[stf["exam"] == exam]

                    # Tabs for mobile-friendly layout
                    tab1, tab2, tab3, tab4 = st.tabs([
                        "Exam",
                        "Subject wise Marks",
                        "Rank Distribution",
                        "Attendance"
                    ])

                    # -------- TAB 1: EXAM --------
                    with tab1:
                        st.subheader("Exam Details")
                        st.metric("Class", selected_class)
                        st.metric("Section", selected_section)
                        st.metric("Student", selected_student)
                        st.metric("Exam", exam)
                        st.metric("Subjects Count", edf["subject"].nunique())

                    # -------- TAB 2: SUBJECT MARKS --------
                    with tab2:
                        st.subheader("Subject-wise Marks")
                        fig_sub = px.bar(
                            edf,
                            x="subject",
                            y="marks",
                            text_auto=True
                        )
                        fig_sub.update_traces(textposition="outside")
                        st.plotly_chart(fig_sub, use_container_width=True, key=f"{fig_sub}_{exam}_{i}")

                    # -------- TAB 3: SUBJECT-WISE RANK --------
                    with tab3:
                        st.subheader("Subject-wise Rank within Class")
                        
                        class_exam_df = df[
                            (df["class"] == selected_class) &
                            (df["section"] == selected_section) &
                            (df["exam"] == exam)
                        ].copy()

                        # Rank within each subject
                        class_exam_df["rank"] = class_exam_df.groupby("subject")["marks"] \
                            .rank(ascending=False, method="min")

                        # Filter selected student
                        stu_rank_df = class_exam_df[class_exam_df["student"] == selected_student]

                        # Convert rank so that Rank 1 becomes highest value
                        stu_rank_df["rank_score"] = stu_rank_df["rank"].max() - stu_rank_df["rank"] + 1

                        fig_rank = px.bar(
                            stu_rank_df,
                            x="rank_score",
                            y="subject",
                            orientation='h',
                            text="rank"  # still show actual rank
                        )

                        fig_rank.update_traces(textposition="outside")
                        #fig_rank.update_layout(xaxis_title="Rank (Higher is Better)")
                        
                        # Hide X-axis to avoid confusion
                        fig_rank.update_layout(xaxis=dict(showticklabels=False, visible=False), xaxis_title=None)


                        st.plotly_chart(fig_rank, use_container_width=True, key=f"rank_{exam}_{i}")

                    # -------- TAB 4: ATTENDANCE --------   
                    with tab4:
                        st.subheader("Attendance (%)")
                        att_value = int(np.random.randint(75, 100))

                        att_df = pd.DataFrame({
                            "type": ["Present", "Absent"],
                            "value": [att_value, 100 - att_value]
                        })

                        # Ensure valid data
                        att_df["value"] = att_df["value"].astype(int)

                        fig_att = px.pie(
                            att_df,
                            names="type",
                            values="value",
                            hole=0.4
                        )

                        # Add labels for clarity
                        fig_att.update_traces(textinfo='percent+label')

                        st.plotly_chart(fig_att, use_container_width=True, key=f"att_{exam}_{i}")

        # Navigation back
        if st.button("⬅ Back to Class"):
            st.session_state.level = "class"
            st.rerun()

        if st.button("⬅ Back to School"):
            st.session_state.level = "school"
            st.rerun()


# ---------------- FOOTER ----------------
st.sidebar.success("Production EI System Live 🚀")
