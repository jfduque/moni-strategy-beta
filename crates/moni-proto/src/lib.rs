pub mod moni {
    pub mod link {
        pub mod v1 {
            tonic::include_proto!("moni.link.v1");
        }
    }
    pub mod store {
        pub mod v1 {
            tonic::include_proto!("moni.store.v1");
        }
    }
    pub mod v1 {
        tonic::include_proto!("moni.v1");
    }
}

pub mod link {
    pub mod v1 {
        pub use crate::moni::link::v1::*;
    }
}

pub mod monitor {
    pub use crate::moni::v1::*;
}

pub mod store {
    pub mod v1 {
        pub use crate::moni::store::v1::*;
    }
}
