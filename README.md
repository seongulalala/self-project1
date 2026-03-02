# Google Merchandise Store GA4 로그 분석
> **구매 전환 저하 원인을 ‘상품 발견 이전 탐색 구조 문제’로 규명한 분석 프로젝트**

## 1. 프로젝트 개요
- **목적:** 첫 방문 세션 내 구매 전환이 멈추는 지점을 찾고, 전환 이전 단계의 병목 원인 규명

- **해결하고자 한 문제:** 구매 전환 저하가 상품/가격 경쟁력의 문제인지, 탐색 환경(UI/UX)의 문제인지 확인

- **사용 데이터:** Google Merchandise Store GA4 BigQuery 공개 로그 (`events_*`)

- **주요 지표 / 분석 기준:** 접속(session_start) 기준 세션 단위 Range 리텐션, Closed 퍼널 분석

**요약**
> 리텐션 및 퍼널 분석을 통해 사용자가 상품을 발견하기 전 단계(session_start → view_item)에서 심각한 이탈 구조를 확인했다. 

> 랜딩 위치별 탐색 경로 분석을 바탕으로, 구매 설득의 문제가 아닌 탐색 구조의 병목 현상임을 증명하고 UI 개선 방향과 KPI 구조를 설계했다.

---

## 2. 데이터 환경 및 분석 도구
- **데이터 출처:** BigQuery GA4 Obfuscated Sample Ecommerce

- **분석 기간:** 2020.11.01 ~ 2020.12.20

- **데이터 건수:** 전체 사용자 수 151,628명

- **주요 기술:** SQL (세션·퍼널·리텐션 추출, 랜딩 정규화, 탐색 경로 모델링)

- **데이터 처리:** 이벤트 시간은 서비스 운영 지역 기준(LA)으로 변환

---

## 3. 핵심 분석 프로세스

### 3-1. 리텐션 및 퍼널 분석
- **리텐션:** 주 단위 리텐션이 빠르게 감소하는 패턴을 확인. 서비스가 재방문보다는 **첫 방문 내 전환 중심 구조**임을 파악했다.

- **퍼널 병목 발견:** 채널, 디바이스, OS 환경과 무관하게 `session_start` 이후 `view_item` 단계에서 전환이 급감했다.

#### ✔ 핵심 퍼널 추출 스니펫 (채널별)
```sql
# (data deleted) 존재 식별 후 필터링해
# 시각화를 위해 도메인을 Self-Referral로 변경해
filtered as (
  select *,
    case when source = 'shop.googlemerchandisestore.com' then "Self-Referral"
    else source
    end as trim_source
  from channel_funnel
  where source != '(data deleted)'
    and medium != '(data deleted)'
)

select
  trim_source,
  source,
  medium,
  session_cnt,
  view_cnt,
  round(safe_divide(view_cnt, session_cnt), 3) as conversion_rate
from filtered
where session_cnt >= 100
order by conversion_rate desc;
```

### 3-2. 랜딩 위치 및 탐색 경로 분석
URL을 페이지 성격에 따라 6개 유형(HOME, CATEGORY, PRODUCT 등)으로 정규화하여 탐색 깊이와 이탈 방식을 분석했다.

HOME 랜딩: 동일 페이지(HOME) 반복 탐색 비중이 압도적. 탐색은 발생하나 상품 영역(PRODUCT)으로 연결되지 않는 구조.

CATEGORY 랜딩: 카테고리 목록 탐색 후 다시 HOME이나 CATEGORY로 회귀. 상품 선택에 실패하고 이탈하는 패턴 확인.

```sql
# 1. 랜딩 페이지 추출해
landing_page_tb as (
  select
    session_key,
    page_location as landing_page
  from (
    select
      session_key,
      page_location,
      event_timestamp,
      row_number() over (partition by session_key order by event_timestamp) as rownum
    from session_key_tb
    where page_location is not null
  )
  where rownum = 1
),

# '/' 루트 경로 처리 및 정규화 작업 진행해
landing_path_normalized as (
  select
    lp.session_key,
    case
      when lp.landing_page is null or lp.landing_page = '' then '/'
      else (
        case
          when cleaned_path = '' then '/'
          when cleaned_path = '/' then '/'
          else regexp_replace(cleaned_path, r'/+$', '')
        end
      )
    end as normalized_landing_page
  from (
    SELECT
      session_key,
      landing_page,
      regexp_replace(
        landing_page,
        r'^https?://(www\.|shop\.)?googlemerchandisestore\.com',
        ''
      ) AS cleaned_path
    from landing_page_tb
  ) lp
)
```
## 4. 인사이트 & 액션 플랜
핵심 인사이트
> **"전환 저하는 가격/상품 문제가 아닌, 상품 발견 이전 탐색 구조의 문제이다."**

HOME: 반복 탐색은 일어나나 상세 페이지 전환은 저조함 (발견 이전 구조적 병목).

CATEGORY: 목록 탐색 후 상품 상세 진입에 실패하고 이탈함 (상품 선택 단계의 문제).

PRODUCT: 단, 상품 상세 진입에 성공한 세션은 결제 완료까지 정상적인 전환 흐름을 보임.

### 액션 제안 및 KPI 설계
발견된 병목 지점을 해소하기 위해 UI 개선 시나리오와 A/B 테스트 목적의 지표를 정의했다.

1. [HOME] 상품 노출 구조 재배치

    목적: 진입 직후 상품 접근 경로 단축

    핵심 KPI: session_start → view_item 전환율

2. [CATEGORY] 썸네일·추천 영역 강화

    목적: 상품 목록 탐색이 실제 클릭과 선택으로 이어지도록 유도

    핵심 KPI: CATEGORY → PRODUCT 이동률

3. [CATEGORY] 상품 정렬 방식 A/B 테스트

    목적: 추천순/인기순 정렬이 상세 진입에 미치는 영향 검증

    핵심 KPI: 정렬 방식별 view_item 전환율 비교