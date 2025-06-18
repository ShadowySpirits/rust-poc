mod unsafe_string {
    use ::std::{
        convert::{AsMut, AsRef},
        fmt::{Display, Formatter, Result as IoResult},
        ops::{Deref, DerefMut},
    };
    #[derive(Default)]
    pub struct UnsafeString(pub String);
    unsafe impl Sync for UnsafeString {}
    unsafe impl Send for UnsafeString {}
    impl Deref for UnsafeString {
        type Target = String;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl DerefMut for UnsafeString {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
    impl AsRef<String> for UnsafeString {
        fn as_ref(&self) -> &String {
            &self.0
        }
    }
    impl AsRef<str> for UnsafeString {
        fn as_ref(&self) -> &str {
            self.0.as_ref()
        }
    }
    impl AsMut<String> for UnsafeString {
        fn as_mut(&mut self) -> &mut String {
            &mut self.0
        }
    }
    impl Display for UnsafeString {
        fn fmt(&self, f: &mut Formatter<'_>) -> IoResult {
            <String as Display>::fmt(&self.0, f)
        }
    }
    impl UnsafeString {
        pub fn new<T: AsRef<str>>(source: T) -> Self {
            Self(String::from(source.as_ref()))
        }
    }
}
use ::std::{
    sync::{Arc, Condvar, Mutex},
    thread,
};
use unsafe_string::UnsafeString;
fn main() {
    // 变更位置一：“互斥锁+条件变量”代替原子变量信号量
    let shared_data = Arc::new((
        Mutex::new(UnsafeString::new("以字符串模拟复杂数据结构")),
        Condvar::new(),
    ));
    let mut join_handles = Vec::new();
    {
        // 线程 3
        let shared_data = Arc::clone(&shared_data);
        join_handles.push(thread::spawn(move || {
            let (lock, cvar) = shared_data.as_ref();
            let _ = lock.lock().and_then(|mut guard| {
                // 变更位置二：以条件变量的阻塞成员方法代替“热轮询自旋锁”阻塞线程 3。
                guard = cvar.wait_while(guard, |guard| {
                    !guard.starts_with("【") && !guard.ends_with("】")
                })?;
                println!("3. 最后打印输出两次修改后的结果: {}", guard);
                Ok(())
            });
        }));
    }
    {
        // 线程 2
        let shared_data = Arc::clone(&shared_data);
        join_handles.push(thread::spawn(move || {
            let (lock, cvar) = shared_data.as_ref();
            let _ = lock.lock().and_then(|mut guard| {
                while !guard.ends_with("】") {
                    // 变更位置三：以条件变量的阻塞成员方法代替 load+acquire 原子操作
                    guard = cvar.wait(guard)?;
                }
                guard.insert(0, '【');
                println!("2. 再添加（首）起始符: {}", guard);
                // 变量位置四：手工地唤起线程 3。“热轮询自旋锁”代码中没有这一步，因为线程 3 会主动
                //             轮询信号量的值是否发生了变更。
                cvar.notify_one();
                Ok(())
            });
        }));
    }
    {
        let shared_data = Arc::clone(&shared_data);
        join_handles.push(thread::spawn(move || {
            let (lock, cvar) = shared_data.as_ref();
            let _ = lock.lock().map(|mut guard| {
                guard.push('】');
                println!("1. 先添加（后）结束符: {}", guard);
                // 变量位置五：手工地唤起线程 2 和 3。“热轮询自旋锁”代码中没有这一步，因为那些线程会主动
                //             轮询信号量的值是否发生了变更。
                cvar.notify_all();
            });
        }));
    }
    for (index, join_handle) in join_handles.into_iter().enumerate() {
        join_handle
            .join()
            .expect(&format!("第{}个线程提前崩溃了", index)[..]);
    }
    println!(
        "4. 结束：{}",
        shared_data
            .0
            .lock()
            .map_or("".to_string(), |guard| guard.to_string())
    );
}
