# Set Assignment
> 2 이상의 파일(xlsx, csv, tsv, jsonl 등)에서 지정한 key column을 기준으로 set operations(diff, intersection, union)을 하는 프로그램 작성

<br>

---

## 2023.03.13
* 과제 내용 확인
* 실행시 프로그램에 필요한 인자값 받는 부분 작성
    * argparse 모듈 사용

---

## 2023.03.14
* 구현 계획
   1. 2개의 엑셀파일에 대해 1개의 key_column에 대한 연산
   2. 2이상의 key_column에 대한 처리(all, any)
   3. 3개 이상의 입력파일에 대한 처리
   4. excel 외의 파일 포맷에 대한 처리
   5. 기존 pandas로 구현한 기능 dask, ploars로 대체
* 2개의 데이터프레임에 대해 각 intersection, diff, union 연산 수행 구현
    <!-- * intersection(교집합)
        - 하나 이상의 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                if key_column == key_columns[0]:
                    inter_df = df1[df1[key_column].isin(df1_set&df2_set)]
                else:
                    inter_df = pd.concat([inter_df, df1[df1[key_column].isin(df1_set&df2_set)]], ignore_index=True).drop_duplicates()
            ~~~
        - 모든 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                df1 = df1[df1[key_column].isin(df1_set&df2_set)]
                df2 = df2[df2[key_column].isin(df1_set&df2_set)]
            inter_df = df1
            ~~~
    * diff(차집합)
        - 하나 이상의 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                df1 = df1[df1[key_column].isin(df1_set-df2_set)]
                df2 = df2[df2[key_column].isin(df2_set-df1_set)]
            diff_df = df1
            ~~~
        - 모든 key column 조건 만족
            ~~~python
            for key_column in key_columns:
                df1_set, df2_set = set(df1[key_column]), set(df2[key_column])
                if key_column == key_columns[0]:
                    diff_df = df1[df1[key_column].isin(df1_set-df2_set)]
                else:
                    diff_df = pd.concat([diff_df, df1[df1[key_column].isin(df1_set-df2_set)]]).drop_duplicates()
            ~~~ -->
    * union(합집합)
        - 하나 이상의 key column 조건 만족
            ~~~python
            union_df = pd.concat([df1, df2]).drop_duplicates(subset=key_columns)
            ~~~
        - 모든 key column 조건 만족
            ~~~python
            union_df = pd.concat([df1, df2])
            for key_column in key_columns:
                union_df = union_df.drop_duplicates(subset=[key_column])            
            ~~~
* 고민사항 및 내일 할 일
    * operation은 한 가지만 입력 가능하도록 예외처리 필요
    * main함수에 있는 코드 추가 검증 후 모듈화
        * 변수, 함수명 등에 대한 작명법 통일 필요 -> 해당 부분 추가 정리할 것
    * 3이상의 input에 대한 처리

---

## 2023.03.15
* 기본 구상
    * setoperator.py 
        - PandasOperator, DaskOperator, PolarsOperator 각각 클래스 구현
        - 각 클래스에서 __call__ 함수를 통해 각 opertion(intersection, diff, union) 결과 dataframe 반환
        - 함수 인자\) DataFrame(iterable), keycolumn(iterable), for_any_key_columns(bool)
        - 함수 결과\) DataFrame
* dataframe 연산 방식 고민
    1. outer merge -> merged dataframe을 각 조건(both, left_only, right_only)에 따라 필터링 -> 필요한 column만 추출
    2. key_column 값을 set로 만들어 조건 생성 -> 원본 dataframe을 각 조건에 따라 필터링(column 값이 해당 조건에 포함되는지) -> key column에 대해 반복문 수행
* 파일 입력 / 파일 저장 별도 구현
    * csv, xlsx
    * tsv 
        - csv 파일과 비슷하지만 ',' 대신 Tab으로 컬럼을 분리하는 파일포맷
    * jsonl 
        - 각 라인이 json객체로 이루어진 구조화된 데이터 형식
        - UTF-8 인코딩 + 각 행이 json 형식 + 각 행이 \n으로 구분
        - 참고 https://jsonlines.org/
        - jsonl to list
            ~~~python
            import json
            def load_jsonl(input_path) -> list:
                data = []
                with open(input_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        data.append(json.loads(line.rstrip('\n|\r')))
                return data
            ~~~
* 내일 할 일
    * getArguments, readFiles, saveFiles 정리 후 이동 -> 외부에서 import하는 형태로
        * xlsx 외의 형식에 대해 sample 만들어서 잘 작동하는지 확인
        * pandas 외의 dataframe framework에서는 방식 다른 점 고려
        * 추후 파일 형식 추가 용이하도록
    * dataframe 연산 방식에 대해 좀 더 고민해보기

---

## 2023.03.16
* A파일과 B파일의 column이 다른 경우 처리
    * 결과 파일의 column은 A파일과 같게
    * union 연산에서 B파일에 A파일의 일부 column이 없을 때 -> 해당 column의 값 비워두기
    * **key column이 존재하지 않는 경우** 어떻게 처리할 지 문제
* (기존 방식)모든 파일을 데이터프레임 형태로 읽어온 후 연산 
    * 연산 시 차례대로 하나씩 불러오는 방식으로 변경
    * 실행 스크립트가 아닌 모듈 내부에서 readdf함수 호출
* union 연산
    * 기준 dataframe에서 key_column값이 중복되는 경우가 있을 수 있어, drop_duplicates 사용 어려움
    * for_any_key_columns O : key_column 값이 하나라도 일치하면 결과값에서 제거
    * for_any_key_columns X : key_column 값이 전부 일치하면 결과값에서 제거
* xlsx 외의 포맷에 대해 테스트 (정상 동작 확인)
    * sample file 만들기
        - csv
            ~~~python
            import csv
            def save_csv(file_name, data):
                with open(file_name, 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerows(data)
            ~~~
        - tsv
            - csv에서 writer의 인자로 delemiter='\t'만 추가
        - jsonl
            ~~~~python
            import json
            def save_jsonl(file_name, data):
            with open(file_name, 'w', encoding='utf-8') as file:
                for json_obj in data:
                    file.write(json.dumps(json_obj) + '\n')
            ~~~~
* getargs, readdf, savedf 함수 위치 조정
* 내일 할 일
    * 지금까지의 진행 내용 정리 후 리뷰
    * key_column이 존재하지 않는 파일 입력받은 경우 처리 방법 고민
    * DASK, polars 공부

---

## 2023.03.17
* [진행상황 정리](https://nifty-aftershave-2ff.notion.site/set-operations-baef0e0acb03448aa8ef363c4d23b5cf) 후 발표
    * 함수명, 변수명은 해당 기능 반영하도록
    * 연산 시 결과로 컬럼 조건을 우선 얻은 후 최종적으로 해당 조건이 반영된 결과 데이터프레임이 출력되는 방식
    * 각 연산에서 공통되는 부분(ex.교집합) 따로 정의 후 이를 재활용하는 방식
    * 파일 입출력 등 타기능을 수행하는 부분은 다른 파일로 분리
    * 부모 클래스 정의한 다음 이를 상속하여 각 프레임워크별 클래스에서 정의하는 방식으로 추상화
* 기존 모듈 파일 분리
    ~~~
    setoperator
     |--  __init__.py
     |--  utils.py
     |--  dataio.py
     `--  setoperator.py
    ~~~
* polars 기본 기능 적용 중
    * 파일 입출력, 데이터프레임 프레임워크 변경, 특정 column값 추출 등

---

## 2023.03.20
* 모든 조건을 정리한 후 eval()함수로 전체 조건 한 번에 적용 => 적용 X
    ~~~python
    condition_list = []
    for input in self.input_list[1:]:
        sub_df = f'readdf("{self.path}/{input}")[{self.key_columns}]'
        for key_column in self.key_columns:
            column_condition = f'main_df["{key_column}"].isin(set({sub_df}["{key_column}"]))'
            condition_list.append(column_condition)
    
    result_df = main_df[eval('|'.join(condition_list))]
    ~~~
    * 시간 증가 
        - intersection, key_columns(group_id, en_id), for_any_key_columns : 8.87s -> 14.47s
* 각 key_column 조건에 대해 reduce함수 적용하여 조건 생성 => 적용 O
    ~~~python
    condition_list = []
    for key_column in self.key_columns:
        condition_list.append(main_df[key_column].isin(set(sub_df[key_column])))
    
    if self.for_any: condition = reduce(lambda x, y: x | y, condition_list)
    else: condition = reduce(lambda x, y: x & y, condition_list)
    ~~~
    * 각 operation 메서드에서 해당 조건 호출하여 필터링 진행
        ~~~python
        # intersection
        result_df = result_df[self._intersection_condition(result_df, sub_df)]
        # diff
        esult_df = result_df[~self._intersection_condition(result_df, sub_df)]
        # union
        sub_df = sub_df[~self._intersection_condition(sub_df, result_df)]        
        result_df = pd.concat([result_df, sub_df])
        ~~~
* polars
    * 기존 pandas 코드에서 문법 수정
* dask
    * pandas 코드 그대로 적용 가능 (합칠 수 없을까?)
    * excel, json 지원 X -> pandas dataframe을 변환하는 방식으로
    * 병렬처리가 특장점인 만큼 해당 내용 활용가능하도록 코드 수정 필요
        - npartitions(파티션 수), num_workers(멀티프로세싱에 사용하는 코어 수) 지정

---

## 2023.03.21
* 10,000,000row csv sample 생성 후 테스트
    * polars는 pandas와 비교 시, 소요시간 약 20%
    * dask는 pandas는 소요시간 비슷하거나 느림 (dask 병렬처리 별도 설정X)
        - 처리속도보다는 메모리 개선에 유용
* dask 이용한 데이터 병렬처리 시도 => 실패...
    * 병렬처리 방식
        - 멀티쓰레드, 멀티프로세스, 로컬클러스터 등
    * 두 데이터프레임간 intersection 연산 시, bool 조건 결과를 각각 파티션에 적용 어려움
        -> 각 파티션에 대한 결과값으로 데이터프레임을 받은 다음 합하는 식으로 시도 가능
        -> 연산 방식을 기존과 아예 다르게 가는 것도 방법
    * 기존 pandas코드에서 dask로 변환 가능한 부분 정확한 구분 필요
* 세미나 관련 내용 정리 중

---

## 2023.03.22
* 진행 내용 정리 중
    * [노션 페이지](https://nifty-aftershave-2ff.notion.site/set-operations-baef0e0acb03448aa8ef363c4d23b5cf)
* dask로 병렬 처리 시 속도 저하 문제
    * 데이터셋 크기가 작은 경우 pandas보다 속도 빠름
    * 데이터셋 크기가 커질 경우 
        - isin 메서드 연산과정에서 전체 데이터프레임이 다수의 파티션으로 분할되어 모든 분할된 데이터셋을 검색하여 비효율적
* modin
    * 엑셀파일 지원x(pandas dataframe 으로 읽은 후 변환)
    * 내일 더 공부해 볼 계획

---