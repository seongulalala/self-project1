# 데이터 설명 (Data Description)

## 1. 데이터 출처
- 출처:  
- 수집 방식:  
- 라이선스 / 제한 사항:  

---

## 2. 데이터 파일 구조
프로젝트에서 사용한 데이터 파일과 주요 목적을 기술합니다.

예)
- travel.csv: 여행지 방문 기록  
- traveler.csv: 사용자 프로필 정보  
- destination.csv: 여행지 정보  

---

## 3. 컬럼 설명 (Column Description)

### traveler.csv
| 컬럼명 | 타입 | 설명 |
|--------|--------|--------|
| traveler_id | int | 사용자 ID |
| age | int | 여행자 나이 |
| gender | object | 'M', 'F', etc |

### travel.csv
| 컬럼명 | 타입 | 설명 |
|--------|--------|--------|
| traveler_id | int | 사용자 ID |
| destination_id | int | 방문한 여행지 |
| duration | int | 체류 일수 |

### destination.csv
| 컬럼명 | 타입 | 설명 |
|--------|--------|--------|
| destination_id | int | 고유 여행지 ID |
| city | object | 도시명 |
| category | object | 여행지 카테고리 (자연/액티비티/문화 등) |

---

## 4. 데이터 크기
- traveler.csv: 10,000 rows  
- travel.csv: 50,000 rows  
- destination.csv: 2,000 rows  

(실제 값으로 수정)

---

## 5. 데이터 품질 이슈
- 결측치: age 일부 결측  
- 이상치: duration 값이 비정상적으로 큰 경우 존재  
- 중복: travel.csv에 중복 방문 존재 가능  

---

## 6. 데이터 전처리 요약
- age 결측치 제거 / 간단한 imputation  
- gender 원-핫 인코딩  
- duration 이상치 제거  
- numeric 컬럼 Standard Scaling  

전처리 상세 내용은 notebooks/02_preprocessing.ipynb 참고.
