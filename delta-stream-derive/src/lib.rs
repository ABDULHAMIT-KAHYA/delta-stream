use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(DeltaState)]
pub fn derive_delta_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let expanded = quote! {
        impl ::delta_stream::schema::DeltaState for #name {
            const SCHEMA_NAME: &'static str = stringify!(#name);
        }
    };
    TokenStream::from(expanded)
}
